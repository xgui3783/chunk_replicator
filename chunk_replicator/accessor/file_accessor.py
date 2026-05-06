from pathlib import Path

from .base import Accessor


class FileAccessor(Accessor):
    can_read = True
    can_write = True

    def __init__(self, base_path: str | Path):
        super().__init__()
        self.base_path = Path(base_path)

    def fetch_file(self, path, offset=0, count=-1):
        with open(self.base_path / path, "rb") as fp:
            fp.seek(offset)
            return fp.read(count)

    def store_file(self, path, b, offset=0):
        (self.base_path / path).parent.mkdir(exist_ok=True, parents=True)
        openmode = "r+b" if (self.base_path / path).exists() else "wb"

        with open(self.base_path / path, openmode) as fp:
            fp.seek(offset)
            fp.write(b)
