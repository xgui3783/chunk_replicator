# Chunk replicator

## Example Usage

```python
from chunk_replicator import (
    EbrainsDataproxyHttpReplicatorAccessor,
    HttpMirrorSrcAccessor,
    User,
    DataProxyBucket,
)
import os
from neuroglancer_scripts.precomputed_io import get_IO_for_existing_dataset

bigbrain_url = "https://neuroglancer.humanbrainproject.eu/precomputed/BigBrainRelease.2015/8bit"
bigbrain_accessor = HttpMirrorSrcAccessor(bigbrain_url)

auth_token=os.getenv("AUTH_TOKEN")
user = User(auth_token=auth_token)
bucket = DataProxyBucket(user, "MY_BUCKET_NAME")

proxy = EbrainsDataproxyHttpReplicatorAccessor(dataproxybucket=bucket, prefix="bigbrain_2015")
bigbrain_accessor.mirror_to(proxy)
```

## License

MIT
