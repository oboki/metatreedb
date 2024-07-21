import logging
import yaml
import shutil
from pathlib import Path


class MetaTree:
    def __init__(self, root, keys: tuple= None, location: dict = None):
        self._root = root
        self._keys = keys
        self._location = location or {}
        # self._has_lock = False
        # self._is_locked = None

    def init(self):
        if not self._exists():
            Path(self.location).mkdir()
            Path(f"{self.location}/metadata.yml").touch()
            Path(f"{self.location}/.metatree").touch()
        elif Path(f"{self.location}/.metatree").exists():
            logging.warning(f"Metatree ({self.location}) is already initialized.")
        else:
            raise Exception(f"Path ({self.location}) already in use.")

    def get(self, location):
        return self._get(location)

    def _get(self, location: dict, create_location_if_not_exists: bool = False):
        for key in self._keys:
            child = location.get(key, None)
            if self._location.get(key, None) is None and child is not None:
                if isinstance(child, dict):
                    if "value" in child:
                        child = child.get("value")
                    elif "metadata" in child:
                        query = child.get("metadata")
                        child = self.metadata.get(query)
                        if child is None:
                            raise Exception(f"{query} not found in metadata.")
                    else:
                        raise Exception(f"Invalid child: {child}")

                next = MetaTree(
                    self._root,
                    self._keys,
                    {key: child, **self._location},
                )

                if create_location_if_not_exists:
                    if not next._exists():
                        Path(next.location).mkdir()
                        next.metadata = {}
                    self.metadata = (
                        dict(children=[child])
                        if self.metadata is None
                        else dict(
                            {
                                k: v
                                for k, v in self.metadata.items()
                                if not k == "children"
                            },
                            children=list(
                                set([child, *self.metadata.get("children", [])])
                            ),
                        )
                    )

                if not next._exists():
                    raise Exception(f"Path ({next.location}) does not exist.")
                if not child in self.metadata.get("children", []):
                    raise Exception(f"Child ({child}) not found in metadata.")

                return next._get(
                    location,
                    create_location_if_not_exists=create_location_if_not_exists,
                )
        return self

    def put(self, location, file=None, force=False):
        dest = self._get(location, create_location_if_not_exists=True)
        file = Path(file)
        if not file.exists():
            raise Exception(f"File ({file}) does not exist.")
        if dest._exists():
            shutil.copy(file, dest.location)
            return Path(f"{dest.location}/{file.name}").exists()

    def list(self):
        return [
            i.name
            for i in Path(self.location).iterdir()
            if not i.name.startswith("metadata.yml")
        ]

    def update(self, **kwargs):
        if "children" in kwargs:
            raise Exception("You cannot update children.")
        self.metadata = dict(self.metadata, **kwargs)

    @classmethod
    def metadata_reader(cls, location):
        filepath = f"{location}/metadata.yml"
        with open(filepath, "r") as file:
            return yaml.safe_load(file)

    @classmethod
    def metadata_writer(cls, location, metadata):
        filepath = f"{location}/metadata.yml"
        with open(filepath, "w") as file:
            yaml.dump(metadata, file)

    def _exists(self):
        return Path(self.location).exists()

    @property
    def location(self):
        ordered_values = []
        for k in self._keys:
            if k in self._location:
                ordered_values.append(self._location.get(k))
        return f"{self._root}/{'/'.join(ordered_values)}"

    @property
    def metadata(self):
        return self.__class__.metadata_reader(self.location)

    @metadata.setter
    def metadata(self, metadata):
        self.__class__.metadata_writer(self.location, metadata)
        self._metadata = metadata
