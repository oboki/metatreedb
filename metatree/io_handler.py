import json
import requests
import shutil

from functools import wraps
from pathlib import Path


class IOHandler:
    _metadata_filename = None
    client = None

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

    def rewrite_location(func):
        @wraps(func)
        def wrapper(cls, location, *args, **kwargs):
            if isinstance(location, Path):
                location = str(location)
            location = location.replace("file://", "")
            return func(cls, location, *args, **kwargs)

        return wrapper

    @classmethod
    @rewrite_location
    def read(cls, location, chunk_size=8192):
        with open(Path(location), "rb") as file:
            while chunk := file.read(chunk_size):
                yield chunk

    @classmethod
    @rewrite_location
    def iterdir(cls, location):
        return [i.name for i in Path(location).iterdir()]

    @classmethod
    @rewrite_location
    def copy(cls, location, filepath):
        shutil.copy(filepath, location)
        return cls.exists(f"{location}/{Path(filepath).name}")

    @classmethod
    @rewrite_location
    def mkdir(cls, location):
        return Path(location).mkdir()

    @classmethod
    @rewrite_location
    def touch(cls, location):
        return Path(location).touch()

    @classmethod
    @rewrite_location
    def unlink(cls, location):
        return Path(location).unlink()

    @classmethod
    @rewrite_location
    def exists(cls, location):
        return Path(location).exists()

    @classmethod
    @rewrite_location
    def to_dict(cls, location, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        try:
            with open(filepath.replace("file://", ""), "rt") as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        except Exception as e:
            raise e

    @classmethod
    @rewrite_location
    def from_dict(cls, location, metadata, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        with open(filepath.replace("file://", ""), "w") as file:
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


class WebHdfsJsonHandler(IOHandler):
    _metadata_filename = "metadata.json"

    def rewrite_location(func):
        @wraps(func)
        def wrapper(cls, location, *args, **kwargs):
            prefix = cls.client.url.replace("http://", "webhdfs://")
            location = location.replace(prefix, "")
            return func(cls, location, *args, **kwargs)

        return wrapper

    @classmethod
    @rewrite_location
    def read(cls, location, chunk_size=8192):
        with cls.client.read(location, chunk_size=chunk_size) as reader:
            for chunk in reader:
                yield chunk

    @classmethod
    @rewrite_location
    def iterdir(cls, location):
        return cls.client.list(location)

    @classmethod
    @rewrite_location
    def copy(cls, location, filepath):
        cls.client.upload(location, filepath)
        return cls.exists(f"{location}/{Path(filepath).name}")

    @classmethod
    @rewrite_location
    def mkdir(cls, location):
        return cls.client.makedirs(location)

    @classmethod
    @rewrite_location
    def touch(cls, location):
        return cls.client.write(location, "")

    @classmethod
    @rewrite_location
    def unlink(cls, location):
        return cls.client.delete(location)

    @classmethod
    @rewrite_location
    def exists(cls, location):
        try:
            cls.client.status(location)
            return True
        except:
            return False

    @classmethod
    @rewrite_location
    def to_dict(cls, location, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        prefix = cls.client.url.replace("http://", "webhdfs://")
        filepath = filepath.replace(prefix, "")
        try:
            return json.loads(b"".join(cls.read(filepath)).decode())
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        except Exception as e:
            raise e

    @classmethod
    @rewrite_location
    def from_dict(cls, location, metadata, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        prefix = cls.client.url.replace("http://", "webhdfs://")
        filepath = filepath.replace(prefix, "")
        cls.client.write(filepath, json.dumps(metadata), overwrite=True)
