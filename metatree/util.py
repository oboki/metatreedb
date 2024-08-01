from functools import wraps
from os.path import expandvars
from pathlib import Path


def with_lock(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self.lock()
        try:
            return func(self, *args, **kwargs)
        finally:
            self.unlock()

    return wrapper


def resolve_file_url(url):
    if url.startswith("file://"):
        url = url.replace("file://", "")
    return f"file://{str(Path(expandvars(url)).resolve())}"
