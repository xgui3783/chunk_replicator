from dataclasses import dataclass
from typing import Any
import json

import numpy as np

from .base import VolumeIO
from chunk_replicator.const import TptInt, SxtInt


@dataclass
class NgScale:
    encoding: str = None
    key: str = None
    voxel_offset: TptInt = TptInt()
    size: TptInt = None
    resolution: TptInt = None
    chunk_sizes: list[TptInt] = None
    compressed_segmentation_block_size: Any = None
    
    scale: Any = None
    width: Any = None
    height: Any = None
    depth: Any = None

    def __post_init__(self):
        resolution = self.resolution
        assert resolution and len(resolution) == 3

        size = self.size
        assert size and len(size) == 3

        assert self.chunk_sizes
        assert len(self.chunk_sizes) == 1
        assert len(self.chunk_sizes[0]) == 3


class NGPVolumeIO(VolumeIO):
    def __init__(self, accessor):
        super().__init__(accessor)
        self.info = json.loads(self.accessor.fetch_file("/info").decode())
        assert all(
            "sharding" not in s for s in self.info.get("scales")
        ), f"reading sharded ngp io is not yet supported"
        self.dict_key_scale = {v["key"]: NgScale(**v) for v in self.info.get("scales")}
        self.dtype = np.dtype(self.info["data_type"])
        self.num_channels = self.info["num_channels"]

    def iter_chunks(self):

        for scale in self.dict_key_scale.values():
            key = scale.key
            size = scale.size
            assert size, f"size not defined for scale: {key}"
            assert len(size) == 3

            chunk_sizes = scale.chunk_sizes
            assert chunk_sizes, f"chunk_sizes not defined for scale: {key}"
            assert (
                len(chunk_sizes) == 1
            ), f"assert len(chunk_sizes) == 1, but got {len(chunk_sizes)}"
            chunk_size = chunk_sizes[0]
            assert (
                len(chunk_size) == 3
            ), f"assert len(chunk_size) == 3, but got {len(chunk_size)}"

            for z_chunk_idx in range((size[2] - 1) // chunk_size[2] + 1):
                for y_chunk_idx in range((size[1] - 1) // chunk_size[1] + 1):
                    for x_chunk_idx in range((size[0] - 1) // chunk_size[0] + 1):
                        yield key, (
                            x_chunk_idx * chunk_size[0],
                            min((x_chunk_idx + 1) * chunk_size[0], size[0]),
                            y_chunk_idx * chunk_size[1],
                            min((y_chunk_idx + 1) * chunk_size[1], size[1]),
                            z_chunk_idx * chunk_size[2],
                            min((z_chunk_idx + 1) * chunk_size[2], size[2]),
                        )

    def read_chunk(self, key, xxyyzz):
        start = xxyyzz[::2]
        end = xxyyzz[1::2]
        _frag = "_".join([f"{a}-{b}" for a, b in zip(start, end)])
        _path = f"/{key}/{_frag}"
        data = self.accessor.fetch_file(_path)

        return np.frombuffer(data, dtype=self.dtype).reshape(
            [e - s for s, e in zip(start, end)][::-1]
        )

    def write_chunk(self, key, xxyyzz: SxtInt, b: np.ndarray):
        start = xxyyzz[::2]
        end = xxyyzz[1::2]

        _frag = "_".join([f"{a}-{b}" for a, b in zip(start, end)])
        _path = f"/{key}/{_frag}"
        self.accessor.store_file(_path, b)
