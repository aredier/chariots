"""google storage integration"""

from typing import Text

from google.cloud import storage
from chariots.base import BaseSaver


class GoogleStorageSaver(BaseSaver):
    """
    saver to persist data mdoels and more to the google storage service.

    :param root_path: the root path of where to save the data inside the bucket
    :param bucket: a google.could.storage `Bucket` object to save the data to
    """

    def __init__(self, root_path: Text, bucket_name: Text, client_kwargs: Optional[Mapping] = None):
        """
        """
        super().__init__(root_path)
        self._client_kwargs = client_kwargs or {}
        self._bucket_name = bucket_name
        storage_client = storage.Client(**self._client_kwargs)
        self.bucket = storage_client.bucket(self._bucket_name)

    def save(self, serialized_object: bytes, path: Text) -> bool:
        blob = self.bucket.blob(path)
        blob.upload_from_string(serialized_object)

    def load(self, path: Text) -> bytes:
        blob = self.bucket.get_blob(path)
        if blob is None:
            raise FileNotFoundError('{} does not exist'.format(path))
        return blob.download_as_string()

    def __getstate__(self):
        self.bucket = None
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state
        storage_client = storage.Client(**self._client_kwargs)
        self.bucket = storage_client.bucket(self._bucket_name)
