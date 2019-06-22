from chariots.core.saving import Serializer, Saver
from chariots.core.versioning import VersionableMeta


OPS_PATH = "/models"


class AbstractOp(metaclass=VersionableMeta):

    def __call__(self, *args, **kwargs):
        raise NotImplementedError("you must define a call for the op to be valid")

    @property
    def name(self):
        return self.__class__.__name__.lower()


class LoadableOp(AbstractOp):

    def __call__(self, *args, **kwargs):
        raise NotImplementedError("you must define a call for the op to be valid")

    @classmethod
    def load(cls, serialized_object: bytes):
        raise NotImplementedError()

    def serialize(self) -> bytes:
        raise NotImplementedError()
