"""tests versioning and the `VersionableMeta` meta-class"""
from chariots.versioning import Version, VersionType, VersionableMeta, VersionedField, VersionedFieldDict


def versioned_class_builder(major=0, minor=0, patch=0):
    """
    generator to build versioned classes, the parameters of the class should modify the version of the
    returned class
    """

    class Versioned(metaclass=VersionableMeta):  # pylint: disable=too-few-public-methods
        """versioned class returned by the closure"""
        my_major = VersionedField(major, VersionType.MAJOR)
        my_minor = VersionedField(minor, VersionType.MINOR)
        my_patch = VersionedField(patch, VersionType.PATCH)

    return Versioned


def versioned_subclass_builder(parent_class, child_major=0, child_minor=0, child_patch=0):
    """fixture to return a subclass of the corresponding verioned class (as would be returned by the
    `versioned_class_builder` with the same parameter)"""

    class VersionedSubclass(parent_class):  # pylint: disable=too-few-public-methods
        """inner class returned"""
        my_other_major = VersionedField(child_major, VersionType.MAJOR)
        my_other_minor = VersionedField(child_minor, VersionType.MINOR)
        my_other_patch = VersionedField(child_patch, VersionType.PATCH)

    return VersionedSubclass


def test_version_change_normal():
    """tests standrad version changes"""
    normal = versioned_class_builder().__version__
    major_change = versioned_class_builder(major=3).__version__
    assert normal.major != major_change.major
    assert normal.minor == major_change.minor
    assert normal.patch == major_change.patch
    assert major_change > normal

    normal = versioned_class_builder().__version__
    major_change = versioned_class_builder(minor=3).__version__
    assert normal.major == major_change.major
    assert normal.minor != major_change.minor
    assert normal.patch == major_change.patch
    assert major_change > normal

    normal = versioned_class_builder().__version__
    major_change = versioned_class_builder(patch=3).__version__
    assert normal.major == major_change.major
    assert normal.minor == major_change.minor
    assert normal.patch != major_change.patch
    assert major_change > normal


def test_inheritance():
    """test version changes with inheritance"""
    parent_class = versioned_class_builder()
    normal = versioned_subclass_builder(parent_class).__version__
    major_change = versioned_subclass_builder(parent_class, child_major=3).__version__
    assert normal.major != major_change.major
    assert normal.minor == major_change.minor
    assert normal.patch == major_change.patch
    assert major_change > normal

    parent_class_changed = versioned_class_builder(major=3)
    normal = versioned_subclass_builder(parent_class).__version__
    major_change = versioned_subclass_builder(parent_class_changed).__version__
    assert normal.major != major_change.major
    assert normal.minor == major_change.minor
    assert normal.patch == major_change.patch
    assert major_change > normal


def test_versioned_field_dict_only_defaults():
    """test verioning behavior when using `VersionedFieldDict`"""
    empty_version = Version()
    versioned_dict = VersionedFieldDict(
        VersionType.MAJOR, {'foo': 3, 'bar': 5}
    )
    assert set(versioned_dict.version_dict) == {'foo', 'bar'}
    assert versioned_dict['foo'] == 3
    assert versioned_dict['bar'] == 5

    # testing foo
    assert versioned_dict.version_dict['foo'] > empty_version
    assert versioned_dict.version_dict['foo'].major != empty_version.major
    assert versioned_dict.version_dict['foo'].minor == empty_version.minor
    assert versioned_dict.version_dict['foo'].patch == empty_version.patch

    # testing bar
    assert versioned_dict.version_dict['bar'] > empty_version
    assert versioned_dict.version_dict['bar'].major != empty_version.major
    assert versioned_dict.version_dict['bar'].minor == empty_version.minor
    assert versioned_dict.version_dict['bar'].patch == empty_version.patch


def test_versioned_field_dict_only_overrides():
    """tets verioned field dict when all the fields are not of the same version type"""
    empty_version = Version()
    versioned_dict = VersionedFieldDict(
        VersionType.MAJOR, {'foo': 3, 'bar': VersionedField(5, VersionType.PATCH)}
    )
    assert set(versioned_dict.version_dict) == {'foo', 'bar'}
    assert versioned_dict['foo'] == 3
    assert versioned_dict['bar'] == 5

    # testing foo
    assert versioned_dict.version_dict['foo'] > empty_version
    assert versioned_dict.version_dict['foo'].major != empty_version.major
    assert versioned_dict.version_dict['foo'].minor == empty_version.minor
    assert versioned_dict.version_dict['foo'].patch == empty_version.patch

    # testing bar
    assert versioned_dict.version_dict['bar'] > empty_version
    assert versioned_dict.version_dict['bar'].major == empty_version.major
    assert versioned_dict.version_dict['bar'].minor == empty_version.minor
    assert versioned_dict.version_dict['bar'].patch != empty_version.patch
