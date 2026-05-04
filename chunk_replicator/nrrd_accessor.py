from functools import cached_property
from tempfile import mkstemp
from gzip import GzipFile
import atexit
import os


from neuroglancer_scripts.file_accessor import FileAccessor

class NRRDReader(FileAccessor):
    can_read = True

    CHUNK_SIZE = 512
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

    def __init__(self, path: str):
        self.path = path
        self.dath: str = None # path.removesuffix(".nrrd") + ".data"
        self.version = None
        self.headers = {}
        self.comments: list[str] = []
        self._nl = b"\n"
        self._init_flag = False
        self._hdr_size = None
        self._init()
        atexit.register(self._at_exit)
    
    def _at_exit(self):
        if self.dath is None:
            return
        os.unlink(self.dath)
        ...


    @cached_property
    def sizes(self):
        return [int(v) for v in self.headers["sizes"].split()]

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
            while not islast:
                acc = b""
                for b in yield_byte():
                    acc += b
                    if acc[-len(self._nl) :] != self._nl:
                        continue
                    islast = process(acc)
                    acc = b""

        _hdr = self._nl.join([f"{k}: {v}".encode() for k, v in self.headers.items()])
        _comments = self._nl.join([c.encode() for c in self.comments])
        _hdr = (
            self.version.encode()
            + self._nl
            + _comments
            + self._nl
            + _hdr
            + self._nl
            + self._nl
        )
        assert self.read(len(_hdr), 2) == b"\x1f\x8b"
        self._hdr_size = len(_hdr)

    def _prepare_read(self):
        if self.dath:
            return
        
        fd, fname = mkstemp(suffix=".nrrd.data")
        match encoding := self.headers["encoding"]:
            case "gz" | "gzip":
                with open(self.path, "rb") as rfp:
                    rfp.seek(self._hdr_size)
                    with open(fname, "wb") as wfp:
                        with GzipFile("r", fileobj=rfp) as gunzip:
                            for b in gunzip:
                                wfp.write(b)
                
            case "raw":
                with open(self.path, "rb") as rfp:
                    rfp.seek(self._hdr_size)
                    with open(fname, "wb") as wfp:
                        for b in rfp:
                            wfp.write(b)
            case _:
                raise Exception(f"encoding {encoding} not supported")
        self.dath = fname

    def read(self, offset, count):
        with open(self.path, "rb") as fp:
            fp.seek(offset)
            return fp.read(count)

    def read_data(self, offset, count):
        self._prepare_read()
        with open(self.dath, "rb") as fp:
            fp.seek(offset)
            return fp.read(count)

    def fetch_chunk(self, key, chunk_coords):
        assert key == "1mm", f"{key} is wrong"
        xmin, xmax, ymin, ymax, zmin, zmax = chunk_coords
        
        _xmin, _xmax = xmin, xmax
        _ymin, _ymax = ymin, ymax

        count = _xmax - _xmin
        main_acc = b""
        for z in range(zmin, zmax):
            for y in range(_ymin, _ymax):
                offset = z * (self.sizes[0] * self.sizes[1]) + y * (self.sizes[0]) + _xmin
                data = self.read_data(offset, count)
                main_acc += data
        return main_acc
