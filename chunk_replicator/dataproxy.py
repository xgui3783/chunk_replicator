from dataclasses import dataclass
import requests

from .user import User

@dataclass
class DataProxyBucket:
    user: User
    bucketname: str
    dataproxy_url: str = "https://data-proxy.ebrains.eu/api/"
    dataproxy_version: str = "v1"
    
    def __post_init__(self):

        if self.bucketname is None:
            raise RuntimeError(f"bucketname cannot be left empty")
        if self.user is None:
            raise RuntimeError(f"user needs to be defined")
        if not isinstance(self.user, User):
            raise RuntimeError(f"user needs to be an instance of User")
        

    def get_object(self, object_name: str, redirect=True):
        raise NotImplementedError
    
    def delete_object(self, object_name: str):
        raise NotImplementedError

    def put_object(self, object_name: str, object: bytes):
        
        response = requests.put(
            f"{self.dataproxy_url}{self.dataproxy_version}/buckets/{self.bucketname}/{object_name}",
            headers={
                "authorization": f"bearer {self.user.auth_token}"
            }
        )
        response.raise_for_status()
        temp_url = response.json().get("url")

        put_resposne = requests.put(
            temp_url,
            data=object
        )
        put_resposne.raise_for_status()
