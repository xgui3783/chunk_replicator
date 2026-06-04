from tqdm import tqdm
import json

from neuroglancer_scripts.accessor import Accessor
from neuroglancer_scripts.sharded_file_accessor import ShardedFileAccessor
from neuroglancer_scripts.file_accessor import FileAccessor

class Mirror:
    def __init__(self, src: Accessor, dst: Accessor):

        assert src.can_read
        assert dst.can_write

        self.src = src
        self.dst = dst
    
    def iter_dst_chunks(self):
        scales = None
        match self.dst:
            case ShardedFileAccessor():
                scales = self.dst.info.get("scales")
            case FileAccessor():
                info = json.loads(self.dst.fetch_file("info"))
                scales = info['scales']
            case _:
                raise NotImplementedError

        for scale in scales:
            key = scale.get('key')
            assert key, f"key not defined"
            
            size = scale.get('size')
            assert size, f"size not defined for scale: {key}"
            # assert len(size) == 3
            
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
            # TODO only run first scale for now
            return 
    
    def mirror(self):
        
        all_chunks = [(k, c) for k, c in self.iter_dst_chunks()]
        progress = tqdm(total=len(all_chunks))
        for k, c in all_chunks:            
            data = self.src.fetch_chunk(k, c)
            self.dst.store_chunk(data, k, c)
            progress.update()
