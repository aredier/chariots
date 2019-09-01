import pytest

import chariots.versioning
import chariots.versioning._version
import chariots.versioning._version_type
import chariots.versioning._versionable_meta
import chariots.versioning._versioned_field
import chariots.versioning._versioned_field_dict
from chariots import versioning


@pytest.fixture()
def versioned_class_builder():
    def builder(major=0, minor=0, patch=0):

        class Versioned(metaclass=chariots.versioning._versionable_meta.VersionableMeta):
            my_major = chariots.versioning._versioned_field.VersionedField(major, chariots.versioning._version_type.VersionType.MAJOR)
            my_minor = chariots.versioning._versioned_field.VersionedField(minor, chariots.versioning._version_type.VersionType.MINOR)
            my_patch = chariots.versioning._versioned_field.VersionedField(patch, chariots.versioning._version_type.VersionType.PATCH)

        return Versioned
    return builder

@pytest.fixture()
def versioned_subclass(versioned_class_builder):
    def builder(parent_major=0, parent_minor=0, parent_patch=0, child_major=0, child_minor=0, child_patch=0):
        ParentClass = versioned_class_builder(parent_major, parent_minor, parent_patch)

        class Versioned(ParentClass):
            my_other_major = chariots.versioning._versioned_field.VersionedField(child_major, chariots.versioning._version_type.VersionType.MAJOR)
            my_other_minor = chariots.versioning._versioned_field.VersionedField(child_minor, chariots.versioning._version_type.VersionType.MINOR)
            my_other_patch = chariots.versioning._versioned_field.VersionedField(child_patch, chariots.versioning._version_type.VersionType.PATCH)

        return Versioned
    return builder


def test_version_change_normal(versioned_class_builder):
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


def test_inheritance(versioned_subclass):
    normal = versioned_subclass().__version__
    major_change = versioned_subclass(child_major=3).__version__
    assert normal.major != major_change.major
    assert normal.minor == major_change.minor
    assert normal.patch == major_change.patch
    assert major_change > normal

    normal = versioned_subclass().__version__
    major_change = versioned_subclass(parent_major=3).__version__
    assert normal.major != major_change.major
    assert normal.minor == major_change.minor
    assert normal.patch == major_change.patch
    assert major_change > normal


def test_versioned_field_dict_only_defaults():
    empty_version = chariots.versioning._version.Version()
    versioned_dict = chariots.versioning._versioned_field_dict.VersionedFieldDict(
        chariots.versioning._version_type.VersionType.MAJOR, {
        "foo": 3,
        "bar": 5
    })
    assert set(versioned_dict.version_dict) == {"foo", "bar"}
    assert versioned_dict["foo"] == 3
    assert versioned_dict["bar"] == 5

    # testing foo
    assert versioned_dict.version_dict["foo"] > empty_version
    assert versioned_dict.version_dict["foo"].major != empty_version.major
    assert versioned_dict.version_dict["foo"].minor == empty_version.minor
    assert versioned_dict.version_dict["foo"].patch == empty_version.patch

    # testing bar
    assert versioned_dict.version_dict["bar"] > empty_version
    assert versioned_dict.version_dict["bar"].major != empty_version.major
    assert versioned_dict.version_dict["bar"].minor == empty_version.minor
    assert versioned_dict.version_dict["bar"].patch == empty_version.patch


def test_versioned_field_dict_only_defaults():
    empty_version = chariots.versioning._version.Version()
    versioned_dict = chariots.versioning._versioned_field_dict.VersionedFieldDict(
        chariots.versioning._version_type.VersionType.MAJOR, {
        "foo": 3,
        "bar": chariots.versioning._versioned_field.VersionedField(5, chariots.versioning._version_type.VersionType.PATCH)
    })
    assert set(versioned_dict.version_dict) == {"foo", "bar"}
    assert versioned_dict["foo"] == 3
    assert versioned_dict["bar"] == 5

    # testing foo
    assert versioned_dict.version_dict["foo"] > empty_version
    assert versioned_dict.version_dict["foo"].major != empty_version.major
    assert versioned_dict.version_dict["foo"].minor == empty_version.minor
    assert versioned_dict.version_dict["foo"].patch == empty_version.patch

    # testing bar
    assert versioned_dict.version_dict["bar"] > empty_version
    assert versioned_dict.version_dict["bar"].major == empty_version.major
    assert versioned_dict.version_dict["bar"].minor == empty_version.minor
    assert versioned_dict.version_dict["bar"].patch != empty_version.patch
