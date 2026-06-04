from functools import cached_property
from collections import defaultdict
import math
import numpy as np
from tempfile import mkstemp
from gzip import GzipFile
from io import BufferedIOBase
from typing import Literal, NamedTuple
import json

from neuroglancer_scripts.file_accessor import FileAccessor


class Metadata(NamedTuple):
    filename: str
    buffer: bytes


class OffsetFp(BufferedIOBase):
    def __init__(self, io: BufferedIOBase, offset: int):
        super().__init__()
        self.io = io
        self.io.seek(offset)
        self.offset = offset

    def read(self, size=-1):
        return self.io.read(size)

    def seek(self, offset, whence=0):
        match whence:
            case 0:
                self.io.seek(offset + self.offset, 0)
            case 1:
                self.io.seek(offset, 1)
            case _:
                raise RuntimeError

    def tell(self):
        return self.io.tell() - self.offset


class NRRDReader(FileAccessor):
    can_read = True

    CHUNK_SIZE = 512
    DTYPE_GROUPINGS = {
        "uint8": {"uchar", "unsigned char", "uint8", "uint8_t"},
        "int8": {"signed char", "int8", "int8_t"},
        "uint16": {
            "ushort",
            "unsigned short",
            "unsigned short int",
            "uint16",
            "uint16_t",
        },
        "int16": {
            "short",
            "short int",
            "signed short",
            "signed short int",
            "int16",
            "int16_t",
        },
        "uint32": {"uint", "unsigned int", "uint32", "uint32_t"},
        "int32": {"int", "signed int", "int32", "int32_t"},
        "uint64": {
            "ulonglong",
            "unsigned long long",
            "unsigned long long int",
            "uint64",
            "uint64_t",
        },
        "int64": {
            "longlong",
            "long long",
            "long long int",
            "signed long long",
            "signed long long int",
            "int64",
            "int64_t",
        },
        "float32": {"float"},
        "float64": {"double"},
    }
    TYPE_TO_BYTE_LENGTH = {
        # 1-byte integers
        "signed char": 1,
        "int8": 1,
        "int8_t": 1,
        "uchar": 1,
        "unsigned char": 1,
        "uint8": 1,
        "uint8_t": 1,
        # 2-byte integers
        "short": 2,
        "short int": 2,
        "signed short": 2,
        "signed short int": 2,
        "int16": 2,
        "int16_t": 2,
        "ushort": 2,
        "unsigned short": 2,
        "unsigned short int": 2,
        "uint16": 2,
        "uint16_t": 2,
        # 4-byte integers
        "int": 4,
        "signed int": 4,
        "int32": 4,
        "int32_t": 4,
        "uint": 4,
        "unsigned int": 4,
        "uint32": 4,
        "uint32_t": 4,
        # 8-byte integers
        "longlong": 8,
        "long long": 8,
        "long long int": 8,
        "signed long long": 8,
        "signed long long int": 8,
        "int64": 8,
        "int64_t": 8,
        "ulonglong": 8,
        "unsigned long long": 8,
        "unsigned long long int": 8,
        "uint64": 8,
        "uint64_t": 8,
        # Floating point
        "float": 4,
        "double": 8,
    }

    def __init__(self, path: str, channel: int = None):
        self.path = path
        self.dath: str = None
        self.version = None
        self.headers = {}
        self.comments: list[str] = []
        self._nl = b"\n"
        self._init_flag = False
        self._hdr_size = None
        self.channel = channel
        self._init()

        self.channel_count = 1
        assert (
            "dimension" in self.headers
        ), "dimension is required header, but was not present"
        self._seen_ch_val = defaultdict(set)
        self._seen_val = set()
        match dim := int(self.headers["dimension"]):
            case 3:
                self.channel_count = 1
            case 4:
                self.channel_count = int(self.headers["sizes"].split()[0])
            case _:
                raise RuntimeError(
                    f"dimension {dim} other than 3 or 4 is not yet supported"
                )
        self.xtract_chan_flag = False
        if self.channel is not None:
            assert self.channel < self.channel_count
            self.xtract_chan_flag = True

    @cached_property
    def sizes(self):
        return [int(v) for v in self.headers["sizes"].split()[-3:]]

    @property
    def byte_size(self):
        return self.TYPE_TO_BYTE_LENGTH[self.headers["type"]]

    def _init(self):

        if self._init_flag:
            return

        self.headers = {}

        with open(self.path, "rb") as fp:
            self.version = fp.read(8).decode()
            if fp.read(1) == b"\r":
                self._nl = b"\r\n"
                assert fp.read(1) == b"\n"

            def yield_byte():
                yield from (v.to_bytes() for v in fp.read(self.CHUNK_SIZE))

            def process(b: bytes):
                """returns bool: is_last"""
                b = b[: -len(self._nl)]

                if b.startswith(b"#"):
                    self.comments.append(b.decode())
                    return False
                if len(b) == 0:
                    return True

                key, value = b.decode().split(":", maxsplit=1)
                assert key not in self.headers
                self.headers[key] = value.lstrip()
                return False

            islast = False
            acc = b""
            _buffered = []
            while not islast:
                _iter = yield_byte()
                for b in _iter:
                    acc += b
                    if acc[-len(self._nl) :] != self._nl:
                        continue

                    islast = process(acc)
                    if islast:
                        _buffered = list(_iter)
                        break
                    acc = b""

            _hdr_size = fp.tell() - len(_buffered)

        assert self.read(_hdr_size, 2) == b"\x1f\x8b"
        self._hdr_size = _hdr_size

    def read(self, offset, count):
        with open(self.path, "rb") as fp:
            fp.seek(offset)
            return fp.read(count)

    def _prepare_data(self):
        _, self.dath = mkstemp(suffix=".data")
        with open(self.dath, "wb") as wfp:

            innerfp = open(self.path, "rb")

            rfp = OffsetFp(innerfp, self._hdr_size)

            for b in GzipFile(fileobj=rfp):
                wfp.write(b)

            innerfp.close()
            rfp.close()

    def read_data(self, offset, count):
        if self.dath is None:
            self._prepare_data()
        with open(self.dath, "rb") as fp:
            fp.seek(offset)
            return fp.read(count)

    def fetch_chunk(self, key, chunk_coords):
        xmin, xmax, ymin, ymax, zmin, zmax = chunk_coords

        _xmin, _xmax = xmin, xmax
        _ymin, _ymax = ymin, ymax

        count = (_xmax - _xmin) * self.channel_count
        channel_offset = self.channel or 0
        main_acc = b""
        for z in range(zmin, zmax):
            for y in range(_ymin, _ymax):
                offset = (
                    z * (self.sizes[0] * self.sizes[1]) + y * (self.sizes[0]) + _xmin
                ) * self.channel_count
                if self.xtract_chan_flag:
                    data = self.read_data(offset, count)[
                        channel_offset :: self.channel_count
                    ]
                else:
                    data = self.read_data(offset, count)
                main_acc += data
        return main_acc

    @property
    def _space_directions(self):
        self._init()
        kinds = self.headers["kinds"].split()
        assert len(kinds) >= 3
        *_, kinds_x, kinds_y, kinds_z = kinds
        assert [kinds_x, kinds_y, kinds_z] == ["domain", "domain", "domain"], (
            "cannot parse '%s'" % self.headers["kinds"]
        )
        space_directions = self.headers["space directions"].split()
        assert len(space_directions) == len(kinds)
        *_, sd_x, sd_y, sd_z = space_directions

        def parse_tuple_str(inpt: str):
            return [float(v) for v in inpt.lstrip("(").rstrip(")").split(",")]

        return [parse_tuple_str(v) for v in [sd_x, sd_y, sd_z]]

    @property
    def _space_origin(self):
        self._init()
        space_origin: str = self.headers["space origin"]
        space_origin = [
            float(v) for v in space_origin.lstrip("(").rstrip(")").split(",")
        ]
        return space_origin

    def generate_info(self, type: Literal["image", "segmentation"] = "image"):

        finest_scale = [np.linalg.norm(v) for v in self._space_directions]
        sizes = [int(v) for v in self.headers["sizes"].split()]

        def get_scale():
            scale = 0
            while (_sizes := [math.ceil(s / (2**scale)) for s in sizes[-3:]]) and any(
                s > 64 for s in _sizes
            ):
                yield {
                    "chunk_sizes": [[64, 64, 64]],
                    "encoding": "raw",
                    "key": f"{scale}",
                    "resolution": [
                        finest_scale[idx] * (2**scale) * 1e3 for idx in range(3)
                    ],
                    "sharding": {
                        "@type": "neuroglancer_uint64_sharded_v1",
                        "data_encoding": "gzip",
                        "hash": "identity",
                        "minishard_bits": 1,
                        "minishard_index_encoding": "gzip",
                        "preshift_bits": 0,
                        "shard_bits": 1,
                    },
                    "size": _sizes,
                    "voxel_offset": [0, 0, 0],
                }
                scale += 1

        for dtype, groups in self.DTYPE_GROUPINGS.items():
            if self.headers["type"] in groups:
                break
        else:
            raise Exception("%s cannot be decoded" % self.headers["type"])

        info = {
            "data_type": dtype,
            "num_channels": 1 if self.xtract_chan_flag else self.channel_count,
            "scales": list(get_scale()),
            "type": type,
        }
        return info

    def generate_transform(self):
        affine = np.eye(4)
        _3x3 = np.array(
            [np.array(d) / np.linalg.norm(d) for d in self._space_directions]
        )
        _3x3 = np.linalg.inv(_3x3)
        affine[:3, :3] = _3x3
        affine[:3, 3] = np.array(self._space_origin) * 1e3
        return affine.tolist()

    def iter_metadata(self, type: Literal["image", "segmentation"] = "image"):
        yield Metadata("info", json.dumps(self.generate_info(type), indent=2).encode())
        xform = self.generate_transform()
        yield Metadata("transform.json", json.dumps(xform, indent=2).encode())
        yield Metadata(
            "meta.json", json.dumps({"version": 1, "transform": xform}, indent=2).encode()
        )
