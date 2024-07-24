import requests
import shutil
import yaml
from pathlib import Path


class IOHandler:
    _metadata_filename = None

    @classmethod
    def iterdir(cls, location):
        raise NotImplementedError

    @classmethod
    def to_dict(cls, location):
        raise NotImplementedError

    @classmethod
    def from_dict(cls):
        raise NotImplementedError

    @classmethod
    def touch(cls, location):
        raise NotImplementedError

    @classmethod
    def exists(cls, location):
        raise NotImplementedError

    @classmethod
    def mkdir(cls, location):
        raise NotImplementedError

    @classmethod
    def read(cls, location):
        raise NotImplementedError


class LocalYamlHandler(IOHandler):
    _metadata_filename = "metadata.yaml"

    @classmethod
    def read(cls, location, chunk_size=8192):
        with open(Path(location), "rb") as file:
            while chunk := file.read(chunk_size):
                yield chunk

    @classmethod
    def iterdir(cls, location):
        return [i.name for i in Path(location).iterdir()]

    @classmethod
    def copy(cls, src, dst):
        shutil.copy(src, dst)
        return cls.exists(f"{dst}/{Path(src).name}")

    @classmethod
    def mkdir(cls, location):
        return Path(location).mkdir()

    @classmethod
    def touch(cls, location):
        return Path(location).touch()

    @classmethod
    def exists(cls, location):
        return Path(location).exists()

    @classmethod
    def to_dict(cls, location):
        filepath = f"{location}/{cls._metadata_filename}"
        with open(filepath, "r") as file:
            return yaml.safe_load(file)

    @classmethod
    def from_dict(cls, location, metadata):
        filepath = f"{location}/{cls._metadata_filename}"
        with open(filepath, "w") as file:
            yaml.dump(metadata, file)


class HttpJsonHandler(IOHandler):
    _metadata_filename = "metadata.json"

    @classmethod
    def read(cls, location, chunk_size=8192):
        with requests.get(location, stream=True) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=chunk_size):
                yield chunk

    @classmethod
    def iterdir(cls, location):
        return requests.get(location).json()

    @classmethod
    def copy(cls, src, dst):
        response = requests.get(src)
        with open(dst, "wb") as file:
            file.write(response.content)
        return cls.exists(dst)

    @classmethod
    def mkdir(cls, location):
        return requests.put(location)

    @classmethod
    def touch(cls, location):
        return requests.post(location)

    @classmethod
    def exists(cls, location):
        return requests.get(location).ok

    @classmethod
    def to_dict(cls, location):
        return requests.get(f"{location}/{cls._metadata_filename}").json()

    @classmethod
    def from_dict(cls, location, metadata):
        return requests.post(location, json=metadata)
