from typing import Iterable
from abc import ABC, abstractmethod

import numpy as np

from chunk_replicator.accessor import Accessor
from chunk_replicator.const import SxtInt


class VolumeIO(ABC):

    def __init__(self, accessor: Accessor):
        super().__init__()
        self.accessor = accessor

    @abstractmethod
    def iter_chunks(self) -> Iterable[tuple[str, SxtInt]]:
        raise NotImplementedError

    @property
    def can_read(self):
        return self.accessor.can_read

    @property
    def can_write(self):
        return self.accessor.can_write

    @abstractmethod
    def read_chunk(self, key: str, xxyyzz: SxtInt) -> np.ndarray:
        raise NotImplementedError

    @abstractmethod
    def write_chunk(self, key: str, xxyyzz: SxtInt, b: np.ndarray) -> None:
        raise NotImplementedError
