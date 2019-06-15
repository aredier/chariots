"""
module that takes care of the versioning of Ops
"""
import hashlib
import enum
from typing import Any


class VersionType(enum.Enum):
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"


class VersionableMeta(type):
    """
    meta class for all versioned objects in the library. It works buy
    """

    def __init__(cls, clsname, superclasses, attributedict):

        versioned_fields = {}
        for attr_name, attr_value in attributedict.items():
            if isinstance(attr_value, VersionedField):
                versioned_fields.setdefault(attr_value.affected_version, []).append(attr_value.__chariots_hash__)
        for super in superclasses:
            if isinstance(super, VersionableMeta):
                for version_type, version_hash in super.__version__.items():
                    versioned_fields.setdefault(version_type, []).append(version_hash)
        cls.__version__ = {version_type: hashlib.sha1("".join(concerned_versioned_fields).encode("utf-8")).hexdigest()
                           for version_type, concerned_versioned_fields in versioned_fields.items()}


class VersionedField:

    def __init__(self, value: Any, affected_version: VersionType, ):
        self.value = value
        self.affected_version = affected_version

    def __set__(self, instance, value):
        self.value = value

    def __get__(self, instance, owner):
        return self.value

    @property
    def __chariots_hash__(self):
        return hashlib.sha1(str(self.value).encode("utf-8")).hexdigest()
