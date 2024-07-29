import requests
import shutil
import json
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
    def from_dict(cls, location, metadata, filepath=None):
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

    @classmethod
    def unlink(cls, location):
        raise NotImplementedError


class LocalJsonHandler(IOHandler):
    _metadata_filename = "metadata.json"

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
    def unlink(cls, location):
        return Path(location).unlink()

    @classmethod
    def exists(cls, location):
        return Path(location).exists()

    @classmethod
    def to_dict(cls, location, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        with open(filepath, "rt") as file:
            try:
                with open(filepath, "rt") as file:
                    return json.load(file)
            except (FileNotFoundError, json.JSONDecodeError):
                return {}
            except Exception as e:
                raise e

    @classmethod
    def from_dict(cls, location, metadata, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        with open(filepath, "w") as file:
            json.dump(metadata, file)


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
    def unlink(cls, location):
        return requests.delete(location)

    @classmethod
    def exists(cls, location):
        return requests.get(location).ok

    @classmethod
    def to_dict(cls, location, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        return requests.get(f"{location}/{cls._metadata_filename}").json()

    @classmethod
    def from_dict(cls, location, metadata, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        return requests.post(filepath, json=metadata)
