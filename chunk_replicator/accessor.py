from typing import List, Set
from neuroglancer_scripts.accessor import Accessor, _CHUNK_PATTERN_FLAT
from neuroglancer_scripts.http_accessor import HttpAccessor
from neuroglancer_scripts.precomputed_io import get_IO_for_existing_dataset
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
import os
import json

from .dataproxy import DataProxyBucket
from .util import retry
from .logger import logger

try:
    WORKER_THREADS = int(os.getenv("WORKER_THREADS", "16"))
except ValueError:
    WORKER_THREADS = 16

class MirrorSrcAccessor(Accessor):
    is_mirror_src = False
    is_mirror_dst = False

    def mirror_to(self, src: Accessor):
        raise NotImplementedError


class HttpMirrorSrcAccessor(HttpAccessor, MirrorSrcAccessor):
    is_mirror_src = True

    def mirror_chunk(self, dst: Accessor, key: str, chunk_coords):
        chunk = self.fetch_chunk(key, chunk_coords)
        dst.store_chunk(chunk, key, chunk_coords)
    
    def mirror_file(self, dst: Accessor, relative_path: str, mime_type="application/octet-stream", fail_fast: bool=False):
        try:
            file_content = self.fetch_file(relative_path)
            dst.store_file(relative_path, file_content, mime_type)
            if relative_path.endswith(":0"):
                dst.store_file(relative_path[:-2], file_content, mime_type)
        except Exception as e:
            if fail_fast:
                raise e
            logger.warn(f"mirror_file {relative_path} failed. fail_fast flag not set, continue...")

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


    def mirror_to(self, dst: Accessor, *, mesh_indicies: List[int]=None):
        assert dst.can_write
        io = get_IO_for_existing_dataset(self)

        logger.debug("Begin mirroring. Got info:", io.info)

        if mesh_indicies is not None:
            logger.debug("mesh_indicies provided, mirroring meshes...")
            self.mirror_meshes(dst, mesh_indicies=mesh_indicies)

        logger.debug("Mirroring info ...")
        dst.store_file("info", json.dumps(io.info).encode("utf-8"), mime_type="application/json")

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

            should_check_chunk_exists = hasattr(dst, "chunk_exists") and callable(dst.chunk_exists)

            chunk_coords = [
                (
                    x_chunk_idx * chunk_size[0], min((x_chunk_idx + 1) * chunk_size[0], size[0]),
                    y_chunk_idx * chunk_size[1], min((y_chunk_idx + 1) * chunk_size[1], size[1]),
                    z_chunk_idx * chunk_size[2], min((z_chunk_idx + 1) * chunk_size[2], size[2]),
                )
                for z_chunk_idx in range((size[2] - 1) // chunk_size[2] + 1)
                for y_chunk_idx in range((size[1] - 1) // chunk_size[1] + 1)
                for x_chunk_idx in range((size[0] - 1) // chunk_size[0] + 1)
            ]

            filtered_chunk_coords = [
                chunk_coord
                for chunk_coord in chunk_coords
                if not should_check_chunk_exists or not dst.chunk_exists(key, chunk_coord)
            ]
            
            logger.debug(f"Mirroring data for key {key}")
            
            with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
                for progress in tqdm(
                    executor.map(
                        self.mirror_chunk,
                        repeat(dst),
                        repeat(key),
                        (chunk_coord for chunk_coord in filtered_chunk_coords),
                    ),
                    total=len(filtered_chunk_coords),
                    desc="writing",
                    unit="chunks",
                    leave=True,
                ):
                    ...


class EbrainsDataproxyHttpReplicatorAccessor(Accessor):
    can_read = False
    can_write = True
    noop = False

    prefix: str
    gzip: bool = False
    flat: bool = True

    dataproxybucket: DataProxyBucket

    _existing_obj_name_set: Set[str] = set()

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
        object_name = relative_path
        if self.prefix:
            object_name = f"{self.prefix}/{object_name}"

        dataproxybucket = self.dataproxybucket
        retry(lambda: dataproxybucket.put_object(
            object_name,
            buf
        ))
    
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
