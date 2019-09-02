import hashlib
from typing import Any, Text

from ._version_type import VersionType


class VersionedField:
    """
    a descriptor to mark that a certain class attribute has to be incorporated in a subversion
    a versioned field is used as a normal class attribute (when gotten it returns the inner value) but is used to
    generate the version of the class it is used on when said class is created (at import time)

    .. testsetup::

        >>> from chariots.versioning import VersionableMeta, VersionType, VersionedField

    .. doctest::

        >>> class MyVersionedClass(metaclass=VersionableMeta):
        ...     foo = VersionedField(3, VersionType.MINOR)
        >>> MyVersionedClass.foo
        3

    :param value: the inner value to be given the field whcih will be returned when you try to get the class attribute
    :param affected_version: the subversion this class attribute has to affect
    """

    def __init__(self, value: Any, affected_version: VersionType, ):
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
