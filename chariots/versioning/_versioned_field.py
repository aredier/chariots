import hashlib
from typing import Any, Text

from ._version_type import VersionType


class VersionedField:
    """
    a versioned field is used as a normal class attribute (when gotten it returns the inner value) but is used to
    generate the version of the class it is used on

    .. testsetup::

        >>> from chariots.versioning import VersionableMeta, VersionType, VersionedField

    .. doctest::

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
