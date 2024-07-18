import yaml


class MetaTree:
    def __init__(self, root, keys: list = None, location: dict = None):
        self._root = root
        self._keys = keys
        self._location = location or {}
        self._has_lock = False
        self._is_locked = None

    def search(self, location):
        for key in self._keys:
            if (
                location.get(key, None) is not None
                and self._location.get(key, None) is None
            ):
                self._location.update({key: location.get(key, None)})
                t = MetaTree(self._root, self._keys, self._location)
                return t.search(location)
        return self
    
    @classmethod
    def metadata_reader(cls, location):
        filepath = f"{location}/metadata.yml"
        with open(filepath, "r") as file:
            return yaml.safe_load(file)
    
    @property
    def location(self):
        return f"{self._root}/{'/'.join(self._location.values())}"

    @property
    def metadata(self):
        return self.__class__.metadata_reader(self.location)

    @metadata.setter
    def metadata(self, value):
        self._metadata = value
