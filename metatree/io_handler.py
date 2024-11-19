import fsspec
import json

from os.path import basename


class IOHandler:
    _metadata_filename = "metadata.json"

    @classmethod
    def read(cls, location, chunk_size=8192, fs: fsspec.AbstractFileSystem = None):
        with fs.open(location, "rb") as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    @classmethod
    def iterdir(cls, location, fs):
        return [basename(l.get("name")) for l in fs.listdir(location)]

    @classmethod
    def copy(cls, location, filepath, fs: fsspec.AbstractFileSystem, recursive=False):
        dst = f"{location}/{basename(filepath)}"
        fs.put(str(filepath), dst, recursive=recursive)
        return cls.exists(dst, fs=fs)

    @classmethod
    def mkdir(cls, location, fs: fsspec.AbstractFileSystem):
        return fs.mkdir(location, exist_ok=True)

    @classmethod
    def touch(cls, location, fs: fsspec.AbstractFileSystem):
        return fs.touch(location)

    @classmethod
    def unlink(cls, location, fs: fsspec.AbstractFileSystem):
        return fs.rm(location)

    @classmethod
    def exists(cls, location, fs: fsspec.AbstractFileSystem):
        return fs.exists(location)

    @classmethod
    def to_dict(cls, location, filepath=None, fs: fsspec.AbstractFileSystem = None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        try:
            with fs.open(filepath, "rt") as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        except Exception as e:
            raise e

    @classmethod
    def from_dict(
        cls,
        location,
        metadata,
        filepath=None,
        fs: fsspec.AbstractFileSystem = None,
    ):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        with fs.open(filepath, "w") as file:
            json.dump(metadata, file)


class LocalJsonHandler(IOHandler): ...


class WebHdfsJsonHandler(IOHandler): ...


class LocalYamlHandler(LocalJsonHandler):
    _metadata_filename = "metadata.yml"


class S3JsonHandler(IOHandler): ...
