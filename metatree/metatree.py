import logging

from pathlib import Path
from time import sleep
from functools import wraps

from .io_handler import LocalJsonHandler, HttpJsonHandler


def with_lock(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self.lock()
        try:
            return func(self, *args, **kwargs)
        finally:
            self.unlock()

    return wrapper


class MetatreeFactory:
    @staticmethod
    def create_instance(root, *args, **kwargs):
        if root.startswith("file://"):
            root = root.replace("file://", "")
        for subclass in Metatree._subclasses:
            if root.startswith(subclass._url_prefix):
                return object.__new__(subclass)
        return Metatree(root, *args, **kwargs)


class Metatree:
    _subclasses = []
    _io_handler = None
    _url_prefix = None
    _locked = None

    def __init_subclass__(cls):
        super().__init_subclass__()
        if cls._url_prefix is not None:
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
        locking_enabled: bool = False,
        **kwargs,
    ):
        self._root = root
        self._keys = keys
        self._location = location or {}
        self._locking_enabled = locking_enabled
        if not kwargs.get("skip_init", False):
            self.init()

    def init(self):
        if not self._exists():
            self._io_handler.mkdir(self.location)
            self._io_handler.touch(
                f"{self.location}/{self._io_handler._metadata_filename}"
            )
            self._io_handler.touch(f"{self._root}/.metatree")
            self.config = dict(keys=self._keys, locking_enabled=self._locking_enabled)
        elif self._io_handler.exists(f"{self._root}/.metatree"):
            self._keys = self.config.get("keys")
            self._locking_enabled = self.config.get("locking_enabled")
            if not self.config.get("keys") == self._keys:
                logging.warning(
                    "Keys are not equal to config. Provided keys will be ignored."
                )
        else:
            raise Exception(f"Path ({self.location}) already in use.")

    def search(self, location):
        self._location = {}
        found, _ = self._search(location)
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
            self._io_handler.mkdir(next.location)
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

    def _search(self, location: dict, create_location_if_not_exists: bool = False):
        if isinstance(location, str):
            location: dict = self.__class__.parse_string_location(location, self._keys)
        for key in self._keys:
            child = location.get(key, None)
            if self._location.get(key, None) is None and child is not None:
                child = self.__class__.parse_child(
                    child,
                    self.metadata,
                )
                next = self.__class__(
                    self._root,
                    location={key: child, **self._location},
                    skip_init=True,
                    **self.config,
                )
                if create_location_if_not_exists:
                    self._create_child_location(next, child)
                if not next._exists():
                    raise Exception(f"Path ({next.location}) does not exist.")
                if not child in self.metadata.get("children", []):
                    raise Exception(f"Child ({child}) not found in metadata.")
                tree, self._location = next._search(
                    location,
                    create_location_if_not_exists=create_location_if_not_exists,
                )
                return tree, self._location
        return self, self._location

    def put(self, location, filepath=None, force=False):
        self._location = {}
        self._search(location, create_location_if_not_exists=True)
        if not self._io_handler.exists(filepath):
            raise Exception(f"File ({filepath}) does not exist.")
        if self._exists():
            if self._io_handler.exists(f"{self.location}/{Path(filepath).name}"):
                raise Exception(f"File ({filepath}) already exists.")
            return self._io_handler.copy(filepath, self.location)

    def list(self):
        return [
            i
            for i in self._io_handler.iterdir(self.location)
            if not i.startswith(self._io_handler._metadata_filename)
        ]

    def get(self, location: str):
        segments = location.strip("/").split("/")
        *base, child = segments
        found = self.search(
            self.__class__.parse_string_location("/".join(base), self._keys)
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
            return self._io_handler.read(f"{found.location}/{child}")

    def update(self, **kwargs):
        if "children" in kwargs:
            raise Exception("You cannot update children.")
        self.metadata = dict(self.metadata, **{k: str(v) for k, v in kwargs.items()})

    def _exists(self):
        return self._io_handler.exists(self.location)

    @property
    def location(self):
        ordered_values = []
        for k in self._keys:
            if k in self._location:
                ordered_values.append(self._location.get(k))
        return f"{self._root}/{'/'.join(ordered_values)}"

    @property
    def metadata(self):
        return self._io_handler.to_dict(self.location)

    @metadata.setter
    @with_lock
    def metadata(self, metadata):
        self._io_handler.from_dict(self.location, metadata)
        self._metadata = metadata

    @property
    def config(self):
        return {
            k: tuple(v) if k == "keys" else v
            for k, v in self._io_handler.to_dict(
                self.location, filepath=f"{self._root}/.metatree"
            ).items()
        }

    @config.setter
    def config(self, config_dict):
        self._io_handler.from_dict(
            self.location,
            {k: list(v) if k == "keys" else v for k, v in config_dict.items()},
            filepath=f"{self._root}/.metatree",
        )

    @property
    def locked(self):
        return self._io_handler.exists(f"{self._root}/.lock")

    def lock(self):
        if not self.config.get("locking_enabled"):
            return True
        attempts = 0
        while attempts < 5 and not self.locked:
            try:
                return self._io_handler.touch(f"{self._root}/.lock")
            except:
                attempts += 1
                sleep(3)

    def unlock(self):
        if not self.config.get("locking_enabled"):
            return True
        return self._io_handler.unlink(f"{self._root}/.lock")


class LocalJsonMetaTree(Metatree):
    _io_handler = LocalJsonHandler
    _url_prefix = "/"

    def __init__(
        self,
        root,
        keys: tuple = None,
        location=None,
        **kwargs,
    ):
        super().__init__(root, keys, location, **kwargs)


class HttpJsonMetaTree(Metatree):
    _io_handler = HttpJsonHandler
    _url_prefix = "http://"

    def __init__(
        self,
        root,
        keys: tuple = None,
        location=None,
        **kwargs,
    ):
        super().__init__(root, keys, location, **kwargs)
