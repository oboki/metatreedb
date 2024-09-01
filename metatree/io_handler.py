import fsspec
import json
import re
import requests
import yaml

from botocore.exceptions import ClientError
from functools import wraps
from os.path import basename
from pathlib import Path


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
    def copy(cls, location, filepath, fs: fsspec.AbstractFileSystem):
        dst = f"{location}/{basename(filepath)}"
        fs.put(filepath, dst)
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


class S3JsonHandler(IOHandler):
    _metadata_filename = "metadata.json"

    def rewrite_location(func):
        @wraps(func)
        def wrapper(cls, location, *args, **kwargs):
            prefix = re.sub(
                r"^https?://", "s3://", cls.client.meta.endpoint_url, count=1
            )
            location = location.replace(prefix, "").strip("/")
            return func(cls, location, *args, **kwargs)

        return wrapper

    @classmethod
    @rewrite_location
    def read(cls, location, chunk_size=8192):
        response = cls.client.get_object(
            Bucket=cls.s3_bucket,
            Key=location,
        )
        body = response.get("Body")
        while True:
            chunk = body.read(chunk_size)
            if not chunk:
                break
            yield chunk

    @classmethod
    @rewrite_location
    def iterdir(cls, location):
        return [
            c.get("Key")
            for c in cls.client.list_objects(
                Bucket=cls.s3_bucket,
                Prefix=f"{location}/",
                Delimiter="/",
            ).get("Contents", [])
        ]

    @classmethod
    @rewrite_location
    def copy(cls, location, filepath):
        cls.client.upload_file(
            Bucket=cls.s3_bucket,
            Key=f"{location}/{Path(filepath).name}",
            Filename=filepath,
        )
        return cls.exists(f"{location}/{Path(filepath).name}")

    @classmethod
    @rewrite_location
    def mkdir(cls, location):
        return cls.client.put_object(
            Bucket=cls.s3_bucket,
            Key=f"{location}/",
        )

    @classmethod
    @rewrite_location
    def touch(cls, location):
        return cls.client.put_object(
            Bucket=cls.s3_bucket,
            Key=location.replace("//", "/"),
            Body=b"",
        )

    @classmethod
    @rewrite_location
    def unlink(cls, location):
        return cls.client.delete_object(
            Bucket=cls.s3_bucket,
            Key=location,
        )

    @classmethod
    @rewrite_location
    def exists(cls, location):
        try:
            cls.client.head_object(
                Bucket=cls.s3_bucket,
                Key=location,
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return len(cls.iterdir(location)) > 0
            else:
                raise

    @classmethod
    @rewrite_location
    def to_dict(cls, location, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        prefix = re.sub(r"^https?://", "s3://", cls.client.meta.endpoint_url, count=1)
        filepath = filepath.replace(prefix, "").strip("/")
        response = cls.client.get_object(
            Bucket=cls.s3_bucket,
            Key=filepath,
        )
        try:
            return json.loads(response.get("Body", {}).read())
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        except Exception as e:
            raise e

    @classmethod
    @rewrite_location
    def from_dict(cls, location, metadata, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        prefix = re.sub(r"^https?://", "s3://", cls.client.meta.endpoint_url, count=1)
        filepath = filepath.replace(prefix, "").strip("/")
        cls.client.put_object(
            Bucket=cls.s3_bucket,
            Key=filepath,
            Body=json.dumps(metadata),
        )
