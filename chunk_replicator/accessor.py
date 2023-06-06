from typing import List, Set, Tuple, Iterator
from neuroglancer_scripts.accessor import Accessor, _CHUNK_PATTERN_FLAT
from neuroglancer_scripts.file_accessor import FileAccessor
from neuroglancer_scripts.http_accessor import HttpAccessor
from neuroglancer_scripts.precomputed_io import get_IO_for_existing_dataset
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
import os
import json
from pathlib import Path
import gzip

from .dataproxy import DataProxyBucket
from .util import retry, retry_dec
from .logger import logger
from .exceptions import NoMeshException

try:
    WORKER_THREADS = int(os.getenv("WORKER_THREADS", "16"))
except ValueError:
    WORKER_THREADS = 16

"""
Type representing Volume Bounding, unpacks to
xmin, xmax, ymin, ymax, zmin, zmax
"""
VBoundType = Tuple[int, int, int, int, int, int]

class MirrorSrcAccessor(Accessor):
    """
    Abstract class, providing mirror_to method
    """
    is_mirror_src = False
    is_mirror_dst = False

    def mirror_metadata(self, dst: Accessor):
        assert dst.can_write

        # mirror /info
        io = get_IO_for_existing_dataset(self)
        dst.store_file("info", json.dumps(io.info).encode("utf-8"), mime_type="application/json", overwrite=True)

        # mirror /transform
        dst.store_file("transform.json", self.fetch_file("transform.json"), mime_type="application/json", overwrite=True)

    def mirror_to(self, dst: Accessor):
        self.mirror_chunks(dst)
        try:
            self.mirror_meshes(dst)
        except NotImplementedError:
            logger.warn(f"mirror mehses for {self.__class__} not implemented. Skipping!")
        except NoMeshException:
            pass

        self.mirror_metadata(dst)

    def mirror_meshes(self, dst: Accessor):
        raise NotImplementedError
    
    def mirror_chunks(self, dst: Accessor):
        with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
            for progress in tqdm(
                executor.map(
                    self.mirror_chunk,
                    repeat(dst),
                    self.iter_chunks()
                ),
                desc="writing",
                unit="chunks",
                leave=True,
            ): pass
    
    def mirror_chunk(self, dst: Accessor, key_chunk_coords: Tuple[str, VBoundType]):
        key, chunk_coords = key_chunk_coords
        if hasattr(dst, "chunk_exists") and callable(dst.chunk_exists):
            if dst.chunk_exists(key, chunk_coords):
                return
        return dst.store_chunk(self.fetch_chunk(key, chunk_coords), key, chunk_coords)

    def iter_chunks(self) -> Iterator[Tuple[str, VBoundType]]:
        """
        Yields all combinations of key, [coord]
        """
        io = get_IO_for_existing_dataset(self)
        for scale in io.info.get("scales", []):
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
            for z_chunk_idx in range((size[2] - 1) // chunk_size[2] + 1):
                for y_chunk_idx in range((size[1] - 1) // chunk_size[1] + 1):
                    for x_chunk_idx in range((size[0] - 1) // chunk_size[0] + 1):
                        yield key, (
                            x_chunk_idx * chunk_size[0], min((x_chunk_idx + 1) * chunk_size[0], size[0]),
                            y_chunk_idx * chunk_size[1], min((y_chunk_idx + 1) * chunk_size[1], size[1]),
                            z_chunk_idx * chunk_size[2], min((z_chunk_idx + 1) * chunk_size[2], size[2]),
                        )


class HttpMirrorSrcAccessor(HttpAccessor, MirrorSrcAccessor):
    is_mirror_src = True

    @retry_dec()
    def mirror_chunk(self, dst: Accessor, key_chunk_coords: Tuple[str, VBoundType]):
        return super().mirror_chunk(dst, key_chunk_coords)
    
    @retry_dec()
    def mirror_file(self, dst: Accessor, relative_path: str, mime_type="application/octet-stream", fail_fast: bool=False):
        try:
            file_content = self.fetch_file(relative_path)
            dst.store_file(relative_path, file_content, mime_type, overwrite=True)
            if relative_path.endswith(":0"):
                dst.store_file(relative_path[:-2], file_content, mime_type, overwrite=True)
        except Exception as e:
            if fail_fast:
                raise
            logger.warn(f"mirror_file {relative_path} failed: {str(e)}. fail_fast flag not set, continue...")

    @retry_dec()
    def mirror_metadata(self, dst: Accessor):
        super().mirror_metadata(dst)

    @retry_dec()
    def mirror_meshes(self, dst: Accessor, *, mesh_indicies: List[int], fail_fast=False):
        assert dst.can_write
        io = get_IO_for_existing_dataset(self)
        mesh_dir = io.info.get("mesh")
        try:
            assert mesh_dir, f"expecting mesh key defined in info, but is not."
        except AssertionError as e:
            if fail_fast:
                raise e
            logger.warn(f"{e}, but fail_fast flag is not set... Skipping")
            return
            
        with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
            for progress in tqdm(
                executor.map(
                    self.mirror_file,
                    repeat(dst),
                    (f"{mesh_dir}/{str(idx)}:0" for idx in mesh_indicies),
                    repeat("application/json"),
                    repeat(fail_fast),
                ),
                total=len(mesh_indicies),
                desc="Fetching and writing mesh metadata...",
                unit="files",
                leave=True,
            ):
                ...
        fragments = [f"{mesh_dir}/{frag}"
                    for idx in mesh_indicies
                    for frag in json.loads(
                        dst.fetch_file(f"{mesh_dir}/{str(idx)}:0").decode("utf-8")
                    ).get("fragments")]

        with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
            for progress in tqdm(
                executor.map(
                    self.mirror_file,
                    repeat(dst),
                    fragments,
                    repeat("application/octet-stream"),
                    repeat(fail_fast),
                ),
                total=len(fragments),
                desc="Fetching and writing meshes...",
                unit="meshes",
                leave=True
            ):
                ...


class EbrainsDataproxyHttpReplicatorAccessor(Accessor):
    can_read = False
    can_write = True
    noop = False

    prefix: str
    gzip: bool = False
    flat: bool = True
    smart_gzip: bool = True

    dataproxybucket: DataProxyBucket

    _existing_obj_name_set: Set[str] = set()

    GZIP_CONTENT_HEADER = {
        'Content-encoding': 'gzip'
    }

    def __init__(self, noop=False, prefix=None, gzip=False, flat=True, smart_gzip=True, dataproxybucket: DataProxyBucket=None) -> None:
        super().__init__()
        
        self.noop = noop
        self.prefix = prefix

        if gzip:
            logger.warn(f"Try smartgzip, you will like it!")

        self.gzip = gzip
        self.flat = flat
        self.smart_gzip = smart_gzip

        self.dataproxybucket = dataproxybucket
        
        if self.dataproxybucket is None:
            raise RuntimeError(f"dataproxybucket cannot be left empty")


    @retry_dec()
    def store_file(self, relative_path, buf, mime_type="application/octet-stream", overwrite=False):
        if self.noop:
            return
        object_name = relative_path
        if self.prefix:
            object_name = f"{self.prefix}/{object_name}"

        enable_gzip = (self.smart_gzip or self.gzip) and mime_type == "application/octet-stream"
        dataproxybucket = self.dataproxybucket
        dataproxybucket.put_object(
            object_name,
            gzip.compress(buf) if enable_gzip else buf,
            headers=self.GZIP_CONTENT_HEADER if enable_gzip else {}
        )
    
    @retry_dec()
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
        dataproxybucket.put_object(
            object_name,
            gzip.compress(buf) if self.smart_gzip or self.gzip else buf,
            headers=self.GZIP_CONTENT_HEADER if self.smart_gzip else {}
        )

    def chunk_exists(self, key, chunk_coords):
        if not self._existing_obj_name_set:

            logger.debug(f"Checking existing objects. Listing existing objects for {self.prefix}...")
            
            for obj in tqdm(
                self.dataproxybucket.iterate_objects(prefix=self.prefix),
                desc="listing",
                unit="objects",
                leave=True
            ):
                self._existing_obj_name_set.add(obj.get("name"))

        object_name = _CHUNK_PATTERN_FLAT.format(
            *chunk_coords,
            key=key,
        )
        if self.prefix:
            object_name = f"{self.prefix}/{object_name}"
        return object_name in self._existing_obj_name_set


class LocalSrcAccessor(FileAccessor, MirrorSrcAccessor):
    is_mirror_src = True

    def mirror_meshes(self, dst: Accessor):
        io = get_IO_for_existing_dataset(self)
        
        mesh_path = io.info.get("mesh")
        if not mesh_path:
            raise NoMeshException
        
        mesh_dir = Path(self.base_path) / mesh_path

        for dirpath, dirnames, filenames in os.walk(mesh_dir):
            for filename in filenames:
                mesh_filename = Path(dirpath, filename)
                output_meshname = Path(mesh_path, filename).with_suffix('')

                buf = None
                mime_type = "application/octet-stream"
                
                if mesh_filename.suffix == ".gz":
                    with gzip.open(mesh_filename,  "rb") as fp:
                        buf = fp.read()
                else:
                    with open(mesh_filename, "rb") as fp:
                        buf = fp.read()
                        try:
                            json.load(fp)
                            mime_type = "application/json"
                        except json.JSONDecodeError:
                            pass

                dst.store_file(output_meshname, buf, mime_type)

__all__ = [
    "LocalSrcAccessor",
    "EbrainsDataproxyHttpReplicatorAccessor",
    "HttpMirrorSrcAccessor"
]
