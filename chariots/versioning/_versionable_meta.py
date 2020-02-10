"""meta class that handles versioning in python"""
import operator
from typing import Mapping, Text

from ._version import Version
from ._versioned_field_dict import VersionedFieldDict
from ._versioned_field import VersionedField


class VersionableMeta(type):
    """
    metaclass for all versioned objects in the library.
    When a new class using this metaclas is created, it will have a `__version__` class attribute that sets all the
    subversions of the class depending on the VersionedFields the class was created with
    """

    def __init__(cls, clsname, superclasses, attributedict):
        super().__init__(clsname, superclasses, attributedict)

        intermediate_versions = {}
        for super_class in reversed(superclasses):
            if not isinstance(super_class, VersionableMeta):
                continue
            intermediate_versions.update(super_class._get_atomic_versions_dict())
        intermediate_versions.update(cls._get_atomic_versions_dict())  # pylint: disable=no-value-for-parameter
        cls.__version__ = sum(
            map(operator.itemgetter(1), sorted(intermediate_versions.items(), key=operator.itemgetter(0))),
            Version()
        )

    def _get_atomic_versions_dict(cls) -> Mapping[Text, 'Version']:
        version_dict = {}
        for attr_name, attr_value in cls.__dict__.items():
            if isinstance(attr_value, VersionedField):
                version_dict[attr_name] = Version().update(attr_value.affected_version,
                                                           attr_value.__chariots_hash__.encode('utf-8'))
            if isinstance(attr_value, VersionedFieldDict):
                version_dict.update(attr_value.version_dict)
        return version_dict
