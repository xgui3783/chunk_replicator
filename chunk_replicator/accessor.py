from typing import List, Set, Tuple, Iterator, Dict
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
from collections import defaultdict

from .dataproxy import DataProxyBucket
from .util import retry_dec
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

    def mirror_metadata(self, dst: Accessor, overwrite=False):
        assert dst.can_write

        # mirror /info
        io = get_IO_for_existing_dataset(self)
        dst.store_file("info", json.dumps(io.info).encode("utf-8"), mime_type="application/json", overwrite=overwrite)

        # mirror /transform
        dst.store_file("transform.json", self.fetch_file("transform.json"), mime_type="application/json", overwrite=overwrite)

    def mirror_to(self, dst: Accessor, overwrite=False):
        self.mirror_chunks(dst, overwrite=overwrite)
        try:
            self.mirror_meshes(dst, overwrite=overwrite)
        except NotImplementedError:
            logger.warn(f"mirror mehses for {self.__class__} not implemented. Skipping!")
        except NoMeshException:
            pass

        self.mirror_metadata(dst, overwrite=overwrite)

    def mirror_meshes(self, dst: Accessor, overwrite=False):
        raise NotImplementedError
    
    def mirror_chunks(self, dst: Accessor, overwrite=False):
        iter_chunks = [val for val in self.iter_chunks()]

        # a bit hacky to populate the existing name set, without calling it once for every thread worker
        if len(iter_chunks) > 0 and not overwrite and hasattr(dst, "chunk_exists") and callable(dst.chunk_exists):
            key, chunk_coord = iter_chunks[0]
            dst.chunk_exists(key, chunk_coord)

        with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
            for progress in tqdm(
                executor.map(
                    self.mirror_chunk,
                    repeat(dst),
                    iter_chunks,
                    repeat(overwrite)
                ),
                desc="writing",
                unit="chunks",
                leave=True,
                total=len(iter_chunks)
            ): pass
    
    def mirror_chunk(self, dst: Accessor, key_chunk_coords: Tuple[str, VBoundType], overwrite):
        key, chunk_coords = key_chunk_coords
        if not overwrite and hasattr(dst, "chunk_exists") and callable(dst.chunk_exists):
            if dst.chunk_exists(key, chunk_coords):
                return
        return dst.store_chunk(self.fetch_chunk(key, chunk_coords), key, chunk_coords, overwrite=overwrite)

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
    def mirror_chunk(self, dst: Accessor, key_chunk_coords: Tuple[str, VBoundType], overwrite):
        return super().mirror_chunk(dst, key_chunk_coords, overwrite)
    
    @retry_dec()
    def mirror_file(self, dst: Accessor, relative_path: str, mime_type="application/octet-stream", fail_fast: bool=False, overwrite=False):
        try:
            file_content = self.fetch_file(relative_path)
            dst.store_file(relative_path, file_content, mime_type, overwrite=overwrite)
            if relative_path.endswith(":0"):
                dst.store_file(relative_path[:-2], file_content, mime_type, overwrite=overwrite)
        except Exception as e:
            if fail_fast:
                raise
            logger.warn(f"mirror_file {relative_path} failed: {str(e)}. fail_fast flag not set, continue...")

    @retry_dec()
    def mirror_metadata(self, dst: Accessor, **kwargs):
        super().mirror_metadata(dst, **kwargs)

    @retry_dec()
    def mirror_meshes(self, dst: Accessor, *, mesh_indicies: List[int], fail_fast=False, ovewrite=False):
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
                    repeat(ovewrite),
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
                    repeat(ovewrite),
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

    _existing_obj_name_map_set: Dict[str, Set[str]] = defaultdict(set)

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

        if self.prefix not in self._existing_obj_name_map_set:

            logger.debug(f"Checking existing objects. Listing existing objects for {self.prefix}...")
            
            for obj in tqdm(
                self.dataproxybucket.iterate_objects(prefix=self.prefix),
                desc="listing",
                unit="objects",
                leave=True
            ):
                self._existing_obj_name_map_set[self.prefix].add(obj.get("name"))


        object_name = _CHUNK_PATTERN_FLAT.format(
            *chunk_coords,
            key=key,
        )
        if self.prefix:
            object_name = f"{self.prefix}/{object_name}"
        return object_name in self._existing_obj_name_map_set[self.prefix]


class LocalSrcAccessor(FileAccessor, MirrorSrcAccessor):
    is_mirror_src = True

    def mirror_mesh(self, dst: Accessor, mesh_filename: Path, dst_mesh_filename: Path):
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

        dst.store_file(dst_mesh_filename, buf, mime_type)


    def mirror_meshes(self, dst: Accessor, overwrite=False):
        io = get_IO_for_existing_dataset(self)
        
        mesh_path = io.info.get("mesh")
        if not mesh_path:
            raise NoMeshException
        
        mesh_dir = Path(self.base_path) / mesh_path

        mesh_params = [
            (Path(dirpath, filename), Path(mesh_path, filename).with_suffix(''))
            for dirpath, dirnames, filenames in os.walk(mesh_dir)
            for filename in filenames
        ]

        with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
            for progress in tqdm(
                executor.map(
                    self.mirror_mesh,
                    repeat(dst),
                    [param[0] for param in mesh_params],
                    [param[1] for param in mesh_params],
                ),
                total=len(mesh_params),
                leave=True,
                unit="meshes",
                desc="writing",
            ): pass


class LocalMeshSrcAccessor(LocalSrcAccessor):
    def mirror_chunk(self, dst: Accessor, key_chunk_coords: Tuple[str, VBoundType], overwrite):
        raise NotImplementedError(f"LocalMeshSrcAccessor do not have access to chunks")

    def mirror_chunks(self, dst: Accessor, overwrite=False):
        raise NotImplementedError(f"LocalMeshSrcAccessor do not have access to chunks")
    
    def mirror_to(self, dst: Accessor, overwrite=False):
        self.mirror_metadata(dst, overwrite=overwrite)
        self.mirror_meshes(dst, overwrite=overwrite)

__all__ = [
    "LocalSrcAccessor",
    "EbrainsDataproxyHttpReplicatorAccessor",
    "HttpMirrorSrcAccessor",
    "LocalMeshSrcAccessor",
]
