"""
module that takes care of the versioning of Ops
"""
import hashlib
import enum
import operator
import time
import collections
from _hashlib import HASH
from typing import Any, Union, Optional, Mapping, Text, Iterator

Hash = Union[HASH, str]


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
        cls.__version__ = sum(
            map(operator.itemgetter(1), sorted(intermediate_versions.items(), key=operator.itemgetter(0))),
            Version()
        )

    def _get_atomic_versions_dict(cls) -> Mapping[Text, "Version"]:
        version_dict = {}
        for attr_name, attr_value in cls.__dict__.items():
            if isinstance(attr_value, VersionedField):
                version_dict[attr_name] = Version().update(attr_value.affected_version,
                                                           attr_value.__chariots_hash__.encode("utf-8"))
            if isinstance(attr_value, VersionedFieldDict):
                version_dict.update(attr_value.version_dict)
        return version_dict


class VersionedField:
    """
    a versioned field is used as a normal class attribute (when gotten it returns the inner value) but is used to
    generate the version of the class it is used on

    >>> class MyVersionedClass(metaclass=VersionableMeta):
    ...     foo = VersionedField(3, VersionType.MINOR)
    >>> MyVersionedClass.foo
    3
    """

    def __init__(self, value: Any, affected_version: VersionType, ):
        """
        :param value: the inner value to be given the field
        :param affected_version: the verssion affected nby this field
        """
        self.value = value
        self.affected_version = affected_version

    def __set__(self, instance, value):
        self.value = value

    def __get__(self, instance, owner):
        return self.value

    @property
    def __chariots_hash__(self) -> Text:
        """
        how to generate the hash (on which the version is built) from the value
        :return: the hash of the inner value
        """
        # TODO find better way to hash
        return hashlib.sha1(str(self.value).encode("utf-8")).hexdigest()


class VersionedFieldDict(collections.MutableMapping):
    """
    a versioned field dict acts as a normal dictionary but the values as interpreted as versioned fields when it is
    a VersionedClass class attribute
    """

    def __init__(self, default_version=VersionType.MAJOR, *args, **kwargs):
        self.default_version = default_version
        self.store = dict()
        self.update(dict(*args, **kwargs))

    def __delitem__(self, key) -> None:
        del self.store[key]

    def __getitem__(self, key) -> Any:
        return self.store[key].value

    def __len__(self) -> int:
        return len(self.store)

    def __iter__(self) -> Iterator[str]:
        return iter(self.store)

    def __setitem__(self, key: str, value: Any) -> None:
        if not isinstance(value, VersionedField):
            value = VersionedField(value, self.default_version)
        if not isinstance(key, str):
            raise TypeError("`VersionedFieldDict` keys must be strings")
        self.store[key] = value

    @property
    def version_dict(self) -> Mapping[str, "Version"]:
        """
        proprety to retrieve the name of the fields and the Versions associated to each of them
        :return: the mapping with the key and the version of the value
        """
        return {attr_name: Version().update(attr_value.affected_version, attr_value.__chariots_hash__.encode("utf-8"))
                for attr_name, attr_value in self.store.items() if isinstance(attr_value, VersionedField)}


class Version:
    """
    a chariot version with three subversions (major, minor, patch)
    each version is a hash of the versioned fields that it is using
    the order of versions is determined by the timestamp of the version creation
    """

    def __init__(self, major: Optional[Hash] = None, minor: Optional[Hash] = None,
                 patch: Optional[Hash] = None, creation_time: Optional[float] = None):
        """
        ONLY PROVIDE ARGUMENTS IF YOU ARE LOADING A VALID VERSION

        :param major: the starting hash of the major version
        :param minor: the starting hash of the minor version
        :param patch: the starting hash of the patch version
        :param creation_time: the starting creation time of the version
        """
        self._major: Hash = major or hashlib.sha1()
        self._minor: Hash = minor or hashlib.sha1()
        self._patch: Hash = patch or hashlib.sha1()
        self._creation_time = creation_time or time.time()

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
    def creation_time(self) -> float:
        """
        the creation time of the op
        :return: the timestamp of the creation time
        """
        return self._creation_time

    def __gt__(self, other: "Version") -> bool:
        if self == other:
            return False
        return self.creation_time > other.creation_time

    def __eq__(self, other: "Version") -> bool:
        return (self.major == other.major and self.minor == other.minor and
                self.patch == other.patch)

    @property
    def major(self) -> str:
        """
        the hash of the major subversion
        :return: the hash string
        """
        if isinstance(self._major, str):
            return self._major
        return self._major.hexdigest()

    @property
    def minor(self) -> str:
        """
        the hash of the minor subversion
        :return: the hash string
        """
        if isinstance(self._minor, str):
            return self._minor
        return self._minor.hexdigest()

    @property
    def patch(self) -> str:
        """
        the hash of the patch subversion
        :return: the hash string
        """
        if isinstance(self._patch, str):
            return self._patch
        return self._patch.hexdigest()

    def update_major(self, input_bytes: bytes) -> "Version":
        """
        updates the major version with some bytes

        :param input_bytes: bytes to update with
        :return: the updated version
        """
        self._major.update(input_bytes)
        return self

    def update_minor(self, input_bytes: bytes) -> "Version":
        """
        updates the minor version with some bytes

        :param input_bytes: bytes to update with
        :return: the updated version
        """
        self._minor.update(input_bytes)
        return self

    def update_patch(self, input_bytes: bytes) -> "Version":
        """
        updates the patch version with some bytes

        :param input_bytes: bytes to update with
        :return: the updated version
        """
        self._patch.update(input_bytes)
        return self

    def update(self, version_type: VersionType, input_bytes: bytes) -> "Version":
        """
        updates the corresponding version of this version with some bytes

        :param version_type: the version to update
        :param input_bytes: the bytes to update with
        :return: the updated version
        """
        if version_type is VersionType.MAJOR:
            return self.update_major(input_bytes)
        if version_type is VersionType.MINOR:
            return self.update_minor(input_bytes)
        if version_type is VersionType.PATCH:
            return self.update_patch(input_bytes)
        raise ValueError(f"you provided an invalid version type: {version_type}")

    def __repr__(self):
        return f"<Version, major:{self.major[:5]}, minor: {self.minor[:5]}, patch: {self.patch[:5]}>"

    def __str__(self):
        hash_str = ".".join((self.major, self.minor, self.patch))
        return "_".join((hash_str, str(self._creation_time)))

    @classmethod
    def parse(cls, version_string: str) -> "Version":
        """
        parses a string representation of a saved version and returns a valid Version object

        :param version_string:
        :return: the version represented by the version string
        """
        hash_str, creation_time = version_string.split("_")
        major, minor, patch = hash_str.split(".")
        return cls(major, minor, patch, float(creation_time))
