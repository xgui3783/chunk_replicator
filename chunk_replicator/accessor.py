from typing import Any, List
from neuroglancer_scripts.accessor import Accessor, _CHUNK_PATTERN_FLAT
from neuroglancer_scripts.http_accessor import HttpAccessor
from neuroglancer_scripts.precomputed_io import get_IO_for_existing_dataset
from tqdm import tqdm

from .dataproxy import DataProxyBucket
from .util import retry

class MirrorSrcAccessor(Accessor):
    is_mirror_src = False
    is_mirror_dst = False

    def mirror_to(self, src: Accessor):
        raise NotImplementedError


class HttpMirrorSrcAccessor(HttpAccessor, MirrorSrcAccessor):
    is_mirror_src = True

    def mirror_to(self, dst: Accessor):
        assert dst.can_write
        io = get_IO_for_existing_dataset(self)
        
        
        for scale in io.info.get('scales'):
            
            key = scale.get('key')
            assert key, f"key not defined"

            size = scale.get('size')
            assert size, f"size not defined for scale: {key}"
            assert len(size) == 3

            chunk_sizes = scale.get('chunk_sizes')
            assert chunk_sizes, f"chunk_sizes not defined for scale: {key}"
            assert len(chunk_sizes) == 1, f"assert len(chunk_sizes) == 1, but got {len(chunk_sizes)}"
            chunk_size = chunk_sizes[0]
            assert len(chunk_size) == 3, f"assert len(chunk_size) == 3, but got {len(chunk_size)}"

            progress_bar = tqdm(
                total=(((size[0] - 1) // chunk_size[0] + 1)
                    * ((size[1] - 1) // chunk_size[1] + 1)
                    * ((size[2] - 1) // chunk_size[2] + 1)),
                desc="writing", unit="chunks", leave=True)

            should_check_chunk_exists = hasattr(dst, "chunk_exists") and callable(dst.chunk_exists)

            # TODO add threading
            for z_chunk_idx in range((size[2] - 1) // chunk_size[2] + 1):
                for y_chunk_idx in range((size[1] - 1) // chunk_size[1] + 1):
                    for x_chunk_idx in range((size[0] - 1) // chunk_size[0] + 1):
                        chunk_coords = (
                            x_chunk_idx * chunk_size[0], min((x_chunk_idx + 1) * chunk_size[0], size[0]),
                            y_chunk_idx * chunk_size[1], min((y_chunk_idx + 1) * chunk_size[1], size[1]),
                            z_chunk_idx * chunk_size[2], min((z_chunk_idx + 1) * chunk_size[2], size[2]),
                        )

                        if should_check_chunk_exists:
                            if dst.chunk_exists(key, chunk_coords):
                                progress_bar.update()
                                continue

                        chunk = self.fetch_chunk(key, chunk_coords)
                        dst.store_chunk(chunk, key, chunk_coords)
                        progress_bar.update()


class EbrainsDataproxyHttpReplicatorAccessor(Accessor):
    can_read = False
    can_write = True
    noop = False

    prefix: str
    gzip: bool = False
    flat: bool = True

    dataproxybucket: DataProxyBucket

    _existing_obj: List[Any] #typeddict with keys: name, bytes, content_type, hash, last_modified

    def __init__(self, noop=False, prefix=None, gzip=False, flat=True, dataproxybucket: DataProxyBucket=None) -> None:
        super().__init__()
        
        self.noop = noop
        self.prefix = prefix

        self.gzip = gzip
        self.flat = flat

        self.dataproxybucket = dataproxybucket
        
        if self.dataproxybucket is None:
            raise RuntimeError(f"dataproxybucket cannot be left empty")

    def store_file(self, relative_path, buf, mime_type="application/octet-stream", overwrite=False):
        if self.noop:
            return
        return super().store_file(relative_path, buf, mime_type, overwrite)
    
    def store_chunk(self, buf, key, chunk_coords, mime_type="application/octet-stream", overwrite=False):
        if self.noop:
            return

        # TODO fix if gzip/flat is defined
        object_name = _CHUNK_PATTERN_FLAT.format(
            *chunk_coords,
            key=key,
        )
        if self.prefix:
            object_name = f"{self.prefix}/{object_name}"

        dataproxybucket = self.dataproxybucket
        retry(lambda: dataproxybucket.put_object(
            object_name,
            buf
        ))

    def chunk_exists(self, key, chunk_coords):
        if not self._existing_obj:
            self._existing_obj = [obj for obj in self.dataproxybucket.iterate_objects()]
        
        object_name = _CHUNK_PATTERN_FLAT.format(
            *chunk_coords,
            key=key,
        )
        if self.prefix:
            object_name = f"{self.prefix}/{object_name}"
        return object_name in [obj.get("name") for obj in self._existing_obj]
