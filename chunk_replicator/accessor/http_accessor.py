from requests import Session, RequestException

from .base import Accessor


class HttpAccessor(Accessor):
    can_write = False
    can_read = True
    _session = Session()

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url

    def store_file(self, path, b, offset=0):
        raise NotImplementedError

    def fetch_file(self, path, offset=0, count=-1):
        end = str(offset + count - 1)
        if count <= -1:
            end = ""
        retry_counter = 64
        while retry_counter > 0:
            try:
                headers = {"Range": f"bytes={offset}-{end}"}
                resp = self._session.get(self.base_url + path, headers=headers)
                resp.raise_for_status()
                return bytes(resp.content)
            except RequestException as e:
                retry_counter -= 1
                print(f"retrying ... {retry_counter}")
