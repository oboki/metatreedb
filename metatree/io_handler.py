import json
import re
import requests
import yaml
import fsspec

from functools import wraps
from pathlib import Path
from botocore.exceptions import ClientError
from os.path import basename


class IOHandler:
    _metadata_filename = "metadata.json"
    fs = fsspec.filesystem("file")
    client = None

    @classmethod
    def read(cls, location, chunk_size=8192):
        with cls.fs.open(location, "rb") as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    @classmethod
    def iterdir(cls, location):
        return [basename(l.get("name")) for l in cls.fs.listdir(location)]

    @classmethod
    def copy(cls, location, filepath):
        dst = f"{location}/{basename(filepath)}"
        cls.fs.copy(filepath, dst)
        return cls.exists(dst)

    @classmethod
    def mkdir(cls, location):
        return cls.fs.mkdir(location, exist_ok=True)

    @classmethod
    def touch(cls, location):
        return cls.fs.touch(location)

    @classmethod
    def unlink(cls, location):
        return cls.fs.rm(location)

    @classmethod
    def exists(cls, location):
        return cls.fs.exists(location)

    @classmethod
    def to_dict(cls, location, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        try:
            with cls.fs.open(filepath, "rt") as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        except Exception as e:
            raise e

    @classmethod
    def from_dict(cls, location, metadata, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        with cls.fs.open(filepath, "w") as file:
            json.dump(metadata, file)


class LocalJsonHandler(IOHandler): ...


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
            prefix = re.sub(r"^https?://", "webhdfs://", cls.client.url, count=1)
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
        prefix = re.sub(r"^https?://", "webhdfs://", cls.client.url, count=1)
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
        prefix = re.sub(r"^https?://", "webhdfs://", cls.client.url, count=1)
        filepath = filepath.replace(prefix, "")
        cls.client.write(filepath, json.dumps(metadata), overwrite=True)


class LocalYamlHandler(LocalJsonHandler):
    _metadata_filename = "metadata.yml"

    @classmethod
    def to_dict(cls, location, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        try:
            with cls.fs.open(filepath, "rt") as file:
                return yaml.safe_load(file)
        except (FileNotFoundError, yaml.YAMLError):
            return {}
        except Exception as e:
            raise e

    @classmethod
    def from_dict(cls, location, metadata, filepath=None):
        if filepath is None:
            filepath = f"{location}/{cls._metadata_filename}"
        with cls.fs.open(filepath, "w") as file:
            yaml.safe_dump(metadata, file)


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
