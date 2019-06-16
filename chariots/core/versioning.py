"""
module that takes care of the versioning of Ops
"""
import hashlib
import enum
import operator
from _hashlib import HASH
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
        super().__init__(clsname, superclasses, attributedict)

        intermediate_versions = {}
        for super_class in reversed(superclasses):
            if not isinstance(super_class, VersionableMeta):
                continue
            intermediate_versions.update(super_class._get_atomic_versions_dict())
        intermediate_versions.update(cls._get_atomic_versions_dict())
        print(intermediate_versions)
        cls.__version__ = sum(
            map(operator.itemgetter(1), sorted(intermediate_versions.items(), key=operator.itemgetter(0))),
            Version()
        )

    def _get_atomic_versions_dict(cls):
        return {attr_name: Version().update(attr_value.affected_version, attr_value.__chariots_hash__.encode("utf-8"))
                for attr_name, attr_value in cls.__dict__.items() if isinstance(attr_value, VersionedField)}


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


class Version:

    def __init__(self):
        self._major: HASH = hashlib.sha1()
        self._minor: HASH = hashlib.sha1()
        self._patch: HASH = hashlib.sha1()

    def __add__(self, other):
        if not isinstance(other, Version):
            raise ValueError(f"can only add Version with version, got {type(other)}")
        result = Version()
        result.update_major(self.major.encode("utf-8"))
        result.update_major(other.major.encode("utf-8"))
        result.update_minor(self.minor.encode("utf-8"))
        result.update_minor(other.minor.encode("utf-8"))
        result.update_patch(self.patch.encode("utf-8"))
        result.update_patch(other.patch.encode("utf-8"))
        return result

    @property
    def major(self):
        return self._major.hexdigest()

    @property
    def minor(self):
        return self._minor.hexdigest()

    @property
    def patch(self):
        return self._patch.hexdigest()

    def update_major(self, input_bytes: bytes):
        self._major.update(input_bytes)
        return self

    def update_minor(self, input_bytes: bytes):
        self._minor.update(input_bytes)
        return self

    def update_patch(self, input_bytes: bytes):
        self._patch.update(input_bytes)
        return self

    def update(self, version_type: VersionType, input_bytes: bytes):
        if version_type is VersionType.MAJOR:
            return self.update_major(input_bytes)
        if version_type is VersionType.MINOR:
            return self.update_minor(input_bytes)
        if version_type is VersionType.PATCH:
            self.update_patch(input_bytes)
        raise ValueError(f"you provided an invalid version type: {version_type}")

    def __str__(self):
        return f"<Version, major:{self.major[:5]}, minor: {self.minor[:5]}, patch: {self.patch[:5]}>"
