from typing import Text

from google.cloud import storage
from chariots.base import BaseSaver


class GoogleStorageSaver(BaseSaver):

    def __init__(self, root_path: Text, bucket: storage.bucket.Bucket):
        super().__init__(root_path)
        self.bucket = bucket

    def save(self, serialized_object: bytes, path: Text) -> bool:
        blob = self.bucket.get_blob(path)
        blob.upload_from_string(serialized_object)

    def load(self, path: Text) -> bytes:
        blob = self.bucket.get_blob(path)
        return blob.download_as_string(path)
