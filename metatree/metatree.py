import logging
from .io_handler import LocalYamlHandler, HttpJsonHandler


class Metatree:
    _io_handler = None
    _subclasses = []
    _url_prefix = None

    def __init_subclass__(cls):
        super().__init_subclass__()
        if cls._url_prefix is not None:
            Metatree._subclasses.append(cls)

    def __new__(cls, root, *args, **kwargs):
        if root.startswith("file://"):
            root = root.replace("file://", "")
        for subclass in cls._subclasses:
            if root.startswith(subclass._url_prefix):
                return object.__new__(subclass)
        return super().__new__(cls)

    def __init__(
        self,
        root,
        keys: tuple = None,
        location: dict = None,
    ):
        self._root = root
        self._keys = keys
        self._location = location or {}

    def init(self):
        if not self._exists():
            self._io_handler.mkdir(self.location)
            self._io_handler.touch(
                f"{self.location}/{self._io_handler._metadata_filename}"
            )
            self._io_handler.touch(f"{self.location}/.metatree")
        elif self._io_handler.exists(f"{self.location}/.metatree")():
            logging.warning(f"Metatree ({self.location}) is already initialized.")
        else:
            raise Exception(f"Path ({self.location}) already in use.")

    def search(self, location):
        self._location = {}
        return self._search(location)

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
                    self._keys,
                    {key: child, **self._location},
                )
                if create_location_if_not_exists:
                    self._create_child_location(next, child)
                if not next._exists():
                    raise Exception(f"Path ({next.location}) does not exist.")
                if not child in self.metadata.get("children", []):
                    raise Exception(f"Child ({child}) not found in metadata.")
                return next._search(
                    location,
                    create_location_if_not_exists=create_location_if_not_exists,
                )
        return self

    def put(self, location, filepath=None, force=False):
        self._location = {}
        dest = self._search(location, create_location_if_not_exists=True)
        if not self._io_handler.exists(filepath):
            raise Exception(f"File ({filepath}) does not exist.")
        if dest._exists():
            return self._io_handler.copy(filepath, dest.location)

    def list(self):
        return [
            i
            for i in self._io_handler.iterdir(self.location)
            if not i.startswith(self._io_handler._metadata_filename)
        ]

    def get(self, location: str):
        segments = location.strip("/").split("/")
        found = self.search("/".join(segments[:-1]))
        if segments[-1] in found.list():
            return self._io_handler.read(f"{self._root}/{location}")

    def update(self, **kwargs):
        if "children" in kwargs:
            raise Exception("You cannot update children.")
        self.metadata = dict(self.metadata, **kwargs)

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
    def metadata(self, metadata):
        self._io_handler.from_dict(self.location, metadata)
        self._metadata = metadata


class LocalYamlMetaTree(Metatree):
    _io_handler = LocalYamlHandler
    _url_prefix = "/"

    def __init__(self, root, keys: tuple = None, location=None):
        super().__init__(root, keys, location)


class HttpJsonMetaTree(Metatree):
    _io_handler = HttpJsonHandler
    _url_prefix = "http://"

    def __init__(self, root, keys: tuple = None, location=None):
        super().__init__(root, keys, location)
