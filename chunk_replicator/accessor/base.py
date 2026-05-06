from typing import Protocol, Literal, ContextManager
from io import BufferedIOBase


class Accessor(Protocol):
    can_read = False
    can_write = False

    def exists(self, path: str) -> bool: ...
    def store_file(self, path: str, b: bytes, offset: int = 0) -> None: ...
    def fetch_file(self, path: str, offset: int = 0, count: int = -1) -> bytes: ...
