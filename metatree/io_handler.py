import json
import requests
import shutil

from functools import wraps
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

    def remove_file_scheme(func):
        @wraps(func)
        def wrapper(cls, location, *args, **kwargs):
            if isinstance(location, Path):
                location = str(location)
            location = location.replace("file://", "")
            return func(cls, location, *args, **kwargs)
        return wrapper

    @classmethod
    @remove_file_scheme
    def read(cls, location, chunk_size=8192):
        with open(Path(location), "rb") as file:
            while chunk := file.read(chunk_size):
                yield chunk

    @classmethod
    @remove_file_scheme
    def iterdir(cls, location):
        return [i.name for i in Path(location).iterdir()]

    @classmethod
    @remove_file_scheme
    def copy(cls, location, filepath):
        shutil.copy(filepath, location)
        return cls.exists(f"{location}/{Path(filepath).name}")

    @classmethod
    @remove_file_scheme
    def mkdir(cls, location):
        return Path(location).mkdir()

    @classmethod
    @remove_file_scheme
    def touch(cls, location):
        return Path(location).touch()

    @classmethod
    @remove_file_scheme
    def unlink(cls, location):
        return Path(location).unlink()

    @classmethod
    @remove_file_scheme
    def exists(cls, location):
        return Path(location).exists()

    @classmethod
    @remove_file_scheme
    def to_dict(cls, location, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        try:
            with open(filepath.replace('file://', ''), "rt") as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        except Exception as e:
            raise e

    @classmethod
    @remove_file_scheme
    def from_dict(cls, location, metadata, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        with open(filepath.replace('file://', ''), "w") as file:
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
