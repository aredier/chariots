"""
The versioning module provides all the types the Chariot's versioning logic is built around. If you want to know more
about the way semantic versioning is handled in Chariots, you can go check out the
:doc:`guiding principles <../principles/versioning>`.

This module is built around the `VersionableMeta` metaclass. This is a very simple metaclas that adds the __version__
class attribute whenever a new versionable class is created:

.. testsetup::

    >>> from chariots.versioning import VersionableMeta, VersionedField, VersionType

.. doctest::

    >>> class MyVersionedClass(metaclass=VersionableMeta):
    ...     pass
    >>> MyVersionedClass.__version__
    <Version, major:da39a, minor: da39a, patch: da39a>

to control the version of your class, you can use `VersionedField` descriptors:

..doctest::

    >>> class MyVersionedClass(metaclass=VersionableMeta):
    ...     foo = VersionedField(3, VersionType.MINOR)
    >>> MyVersionedClass.__version__
    <Version, major:94e72, minor: 36d3c, patch: 94e72>
    >>> MyVersionedClass.foo
    3

and if in a future version of your code, the class attribute changes, the subsequent version will be changed:

..doctest::

    >>> class MyVersionedClass(metaclass=VersionableMeta):
    ...     foo = VersionedField(5, VersionType.MINOR)
    >>> MyVersionedClass.__version__
    <Version, major:94e72, minor: 72101, patch: 94e72>
    >>> MyVersionedClass.foo
    5

but this version change only happen when the class is created and not when you change the value of this class attribute
during the lifetime of your class:

.. doctest::

    >>> MyVersionedClass.foo = 7
    >>> MyVersionedClass.__version__
    <Version, major:94e72, minor: 72101, patch: 94e72>
    >>> MyVersionedClass.foo
    7


This module also provides a helper for creating versioned `dict` (where each value of the `dict` acts as a
`VersionedField`) with the `VersionedFieldDict` descriptors:

.. doctest::

    >>> class MyVersionedClass(metaclass=VersionableMeta):
    ...     versioned_dict = VersionedFieldDict(VersionType.PATCH,{
    ...         'foo': 1,
    ...         'bar': 2,
    ...         'blu': VersionedField(3, VersionType.MAJOR)
    ...     })
    >>> MyVersionedClass.__version__
    <Version, major:ddf7a, minor: 1b365, patch: 68722>
    >>> MyVersionedClass.versioned_dict['foo']
    1
    >>> class MyVersionedClass(metaclass=VersionableMeta):
    ...     versioned_dict = VersionedFieldDict(VersionType.PATCH,{
    ...         'foo': 10,
    ...         'bar': 2,
    ...         'blu': VersionedField(3, VersionType.MAJOR)
    ...     })
    >>> MyVersionedClass.__version__
    <Version, major:ddf7a, minor: 1b365, patch: 18615>
    >>> MyVersionedClass.versioned_dict['foo']
    10
    >>> class MyVersionedClass(metaclass=VersionableMeta):
    ...     versioned_dict = VersionedFieldDict(VersionType.PATCH,{
    ...         'foo': 1,
    ...         'bar': 2,
    ...         'blu': VersionedField(10, VersionType.MAJOR)
    ...     })
    >>> MyVersionedClass.__version__
    <Version, major:d5abf, minor: 1b365, patch: 68722>
    >>> MyVersionedClass.versioned_dict['blu']
    10


this is for instance used for the `model_parameters` attribute of the :doc:`sci-kit learn ops <./chariots.sklearn>`
"""

from ._version import Version
from ._versionable_meta import VersionableMeta
from ._versioned_field import VersionedField
from ._versioned_field_dict import VersionedFieldDict
from ._version_type import VersionType

__all__ = [
    'Version',
    'VersionType',
    'VersionedField',
    'VersionedFieldDict',
    'VersionableMeta',
]
