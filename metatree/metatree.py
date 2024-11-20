import logging

from pathlib import Path
from time import sleep
from urllib.parse import urlparse

import fsspec

from .io_handler import (
    LocalJsonHandler,
    WebHdfsJsonHandler,
    S3JsonHandler,
)
from .util import with_lock, resolve_file_url


class MetatreeFactory:
    @staticmethod
    def create_instance(root, *args, **kwargs):
        parsed = urlparse(root)
        for subclass in Metatree._subclasses:
            if parsed.scheme in subclass._url_scheme:
                return object.__new__(subclass)
        return Metatree(root, *args, **kwargs)


class Metatree:
    _subclasses = []
    _io_handler = None
    _url_scheme = None
    _locked = None
    _fs: fsspec.AbstractFileSystem = None

    def __init_subclass__(cls):
        super().__init_subclass__()
        if cls._url_scheme is not None:
            Metatree._subclasses.append(cls)

    def __new__(cls, root, *args, **kwargs):
        io_handler = kwargs.get("io_handler")
        if io_handler is not None:
            instance = super().__new__(cls)
            instance._io_handler = io_handler
            return instance
        return MetatreeFactory.create_instance(root, *args, **kwargs)

    def __init__(
        self,
        root,
        keys: tuple = None,
        location: dict = None,
        locking_enabled: bool = True,
        **kwargs,
    ):
        _parsed_url = urlparse(root)
        if _parsed_url.scheme in LocalJsonMetaTree._url_scheme:
            root = resolve_file_url(root)
        self._root = root
        self._keys = keys
        self._location = location or {}
        self._locking_enabled = locking_enabled
        self._kwargs = kwargs
        self._io_handler.kwargs = kwargs
        if not kwargs.get("fs", None) is None:
            self._fs = kwargs.get("fs")
        else:
            self._fs = fsspec.filesystem(_parsed_url.scheme, **kwargs)
        if not kwargs.get("skip_init", False):
            self.init()

    def init(self):
        if self._io_handler.exists(f"{self.root}/.metatree", fs=self._fs):
            self._keys = self.config.get("keys")
            self._locking_enabled = self.config.get("locking_enabled")
            if not self.config.get("keys") == self._keys:
                logging.warning(
                    "Keys are not equal to config. Provided keys will be ignored."
                )
        elif not self._exists():
            self._io_handler.mkdir(self.location, fs=self._fs)
            self._io_handler.touch(
                f"{self.location}/{self._io_handler._metadata_filename}",
                fs=self._fs,
            )
            self._io_handler.touch(f"{self.root}/.metatree", fs=self._fs)
            self.config = dict(keys=self._keys, locking_enabled=self._locking_enabled)
        else:
            raise Exception(f"Path ({self.location}) already in use.")

    @property
    def root(self):
        return self._root

    def set_location_to_root(self):
        self._location = {}

    def find(self, location):
        self.set_location_to_root()
        found, _ = self._find(location)
        return found

    @classmethod
    def parse_child(cls, child, metadata):
        if isinstance(child, dict):
            if "value" in child:
                child = child.get("value")
            elif "metadata" in child:
                query = child.get("metadata")
                child = metadata.get(query)
                if child is None:
                    raise Exception(f"{query} not found in metadata.")
            else:
                raise Exception(f"Invalid child: {child}")
        return child

    def _create_child_location(self, next, child):
        if not next._exists():
            self._io_handler.mkdir(next.location, fs=self._fs)
            next.metadata = {}
        self.metadata = (
            dict(children=[child])
            if self.metadata is None
            else dict(
                {k: v for k, v in self.metadata.items() if not k == "children"},
                children=list(set([child, *self.metadata.get("children", [])])),
            )
        )

    @classmethod
    def parse_string_location(cls, location, keys):
        splited = location.strip("/").split("/")
        return {
            keys[k]: (
                {"metadata": p.strip(">").strip("<")}
                if p.endswith(">") and p.startswith("<")
                else {"value": p}
            )
            for k, p in enumerate(splited)
        }

    def _find(self, location: dict, create_location_if_not_exists: bool = False):
        if isinstance(location, str):
            location: dict = self.__class__.parse_string_location(location, self._keys)
        for key in self._keys:
            child = location.get(key, None)
            if self._location.get(key, None) is None and child is not None:
                child = self.__class__.parse_child(
                    child,
                    self.metadata,
                )
                kwargs = dict(
                    self._kwargs,
                    location={key: child, **self._location},
                    skip_init=True,
                    io_handler=self._io_handler,
                    fs=self._fs,
                    **self.config,
                )
                next = self.__class__(
                    self._root,
                    **kwargs,
                )
                if create_location_if_not_exists:
                    self._create_child_location(next, child)
                if not next._exists():
                    raise Exception(f"Path ({next.location}) does not exist.")
                if not child in self.metadata.get("children", []):
                    raise Exception(f"Child ({child}) not found in metadata.")
                tree, self._location = next._find(
                    location,
                    create_location_if_not_exists=create_location_if_not_exists,
                )
                return tree, self._location
        return self, self._location

    def put(self, location, filepath=None, force=False, recursive=False):
        self.set_location_to_root()
        self._find(location, create_location_if_not_exists=True)
        if not Path(filepath).exists():
            raise Exception(f"File ({filepath}) does not exist.")
        if self._exists():
            if self._io_handler.exists(
                f"{self.location}/{Path(filepath).name}", fs=self._fs
            ):
                raise Exception(f"File ({filepath}) already exists.")
            return self._io_handler.copy(
                self.location,
                filepath,
                fs=self._fs,
                recursive=recursive,
            )

    def list(self):
        return [
            i
            for i in self._io_handler.iterdir(self.location, fs=self._fs)
            if not i.startswith(self._io_handler._metadata_filename)
        ]

    def get(self, location: str, outfile: str = None, recursive=False):
        if outfile is not None:
            if Path(outfile).exists():
                raise Exception(f"Path '{outfile}' already exists.")
        segments = location.strip("/").split("/")
        *parent, child = segments
        found = (
            self.find(
                self.__class__.parse_string_location("/".join(parent), self._keys)
            )
            if parent
            else self
        )
        child = self.__class__.parse_child(
            (
                {"metadata": child.strip(">").strip("<")}
                if child.endswith(">") and child.startswith("<")
                else {"value": child}
            ),
            found.metadata,
        )
        if child in found.list():
            if outfile is not None:
                found._fs.download(
                    f"{found.location}/{child}", outfile, recursive=recursive
                )
                if not Path(outfile).exists():
                    raise Exception(f"Download failed.")
            return self._io_handler.read(f"{found.location}/{child}", fs=self._fs)

    def update(self, **kwargs):
        if "children" in kwargs:
            raise Exception("You cannot update children.")
        self.metadata = dict(self.metadata, **{k: str(v) for k, v in kwargs.items()})

    def _exists(self):
        return self._io_handler.exists(self.location, fs=self._fs)

    @property
    def location(self):
        ordered_values = []
        for k in self._keys:
            if k in self._location:
                ordered_values.append(self._location.get(k))
        return f"{self.root}/{'/'.join(ordered_values)}".rstrip("/").replace(
            "webhdfs://", ""
        )

    @property
    def metadata(self):
        return self._io_handler.to_dict(self.location, fs=self._fs)

    @metadata.setter
    @with_lock
    def metadata(self, metadata):
        self._io_handler.from_dict(self.location, metadata, fs=self._fs)
        self._metadata = metadata

    @property
    def config(self):
        return {
            k: tuple(v) if k == "keys" else v
            for k, v in self._io_handler.to_dict(
                self.root,
                filepath=f"{self.root}/.metatree",
                fs=self._fs,
            ).items()
        }

    @config.setter
    def config(self, config_dict):
        self._io_handler.from_dict(
            self.location,
            {k: list(v) if k == "keys" else v for k, v in config_dict.items()},
            filepath=f"{self.root}/.metatree",
            fs=self._fs,
        )

    def lock(self):
        if not self.config.get("locking_enabled"):
            return True
        attempts = 0
        while self._locked is not True:
            if attempts > 5:
                raise Exception("max attempts reached.")
            try:
                if self._io_handler.exists(f"{self.root}/.lock", fs=self._fs):
                    raise Exception("Locking failed.")
                self._locked = True
                return self._io_handler.touch(f"{self.root}/.lock", fs=self._fs)
            except Exception as e:
                logging.warning("Locking failed.")
                attempts += 1
                sleep(3)
        raise Exception("lock failed.")

    def unlock(self):
        if not self.config.get("locking_enabled"):
            return True
        self._locked = None
        return self._io_handler.unlink(f"{self.root}/.lock", fs=self._fs)


class LocalJsonMetaTree(Metatree):
    _io_handler = LocalJsonHandler
    _url_scheme = ["", "file"]

    def __init__(
        self,
        root,
        keys: tuple = None,
        location=None,
        **kwargs,
    ):
        super().__init__(root, keys, location, **kwargs)


class WebHdfsJsonMetaTree(Metatree):
    _io_handler = WebHdfsJsonHandler
    _url_scheme = ["webhdfs"]

    def __init__(
        self,
        root,
        keys: tuple = None,
        location=None,
        **kwargs,
    ):
        super().__init__(root, keys, location, **kwargs)


class S3JsonMetaTree(Metatree):
    _io_handler = S3JsonHandler
    _url_scheme = ["s3"]

    def __init__(
        self,
        root,
        keys: tuple = None,
        location=None,
        **kwargs,
    ):
        super().__init__(root, keys, location, **kwargs)
