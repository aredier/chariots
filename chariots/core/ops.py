from typing import Type, List

from chariots.core.versioning import VersionableMeta, VersionedField, VersionType


class AbstractOp(metaclass=VersionableMeta):

    def __call__(self, *args, **kwargs):
        raise NotImplementedError("you must define a call for the op to be valid")


