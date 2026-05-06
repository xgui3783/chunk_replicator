from concurrent.futures import ThreadPoolExecutor
import json

import numpy as np

from ..base import VolumeIO
from .zarrv3_metadata import Zarr3ArrayMetadata, Zarr3GroupMetadata
from .codecs import TermCodecError


class ZarrV3VolumeIO(VolumeIO):
    def __init__(self, accessor):
        super().__init__(accessor)

        group_metadata_json = json.loads(self.accessor.fetch_file("zarr.json"))
        self.group_metadata = Zarr3GroupMetadata(**group_metadata_json)
        assert len(self.group_metadata.attributes["ome"].multiscales) == 1
        multiscale = self.group_metadata.attributes["ome"].multiscales[0]
        self.array_ome_metadata = {ds.path: ds for ds in multiscale.datasets}

        with ThreadPoolExecutor() as ex:
            self.array_metadata = {
                path: Zarr3ArrayMetadata(**json.loads(b_array_metadata))
                for path, b_array_metadata in zip(
                    self.array_ome_metadata.keys(),
                    ex.map(
                        self.accessor.fetch_file,
                        [f"{p}/zarr.json" for p in self.array_ome_metadata.keys()],
                    ),
                )
            }

    def iter_chunks(self):
        raise NotImplementedError

    def read_chunk(self, key, xxyyzz):
        raise NotImplementedError

    def write_chunk(self, key, xxyyzz, chunk: np.ndarray):

        assert key in self.array_metadata
        array_metadata = self.array_metadata[key]
        path = key + "/" + array_metadata.format_path(xxyyzz)

        chunk = np.transpose(chunk)

        try:
            for codec in array_metadata.codecs:
                chunk = codec.encode(
                    chunk, array_metadata, self, chunk_coords=xxyyzz, path=path
                )

            assert isinstance(chunk, bytes)
            self.accessor.store_file(path, chunk)
        except TermCodecError:
            pass
