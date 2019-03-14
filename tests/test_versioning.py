import copy
import time

import pytest

from chariots.core.ops import BaseOp
from chariots.core.requirements import Number
from chariots.core.versioning import Version
from chariots.core.versioning import VersionField
from chariots.core.versioning import _VersionField
from chariots.core.versioning import VersionType
from chariots.core.versioning import SubVersion
from chariots.core.versioning import SubversionString


@pytest.fixture
def versioned_op_cls():
    class VersionedOp(BaseOp):
        name = "fake_op"
        versioned_field = VersionField(VersionType.MAJOR, default_value=2)
        markers = [Number]
        def _main():
            return self.versioned_field
    return VersionedOp

@pytest.fixture
def downstream_op_cls(versioned_op_cls):
    class FakeDown(BaseOp):
        requires = {"fake_dep": Number}
        name = "fake_down"
        markers = [Number]
        
        def _main(fake_dep):
            pass
    return FakeDown


def test_version_updates():
    version = Version()
    field = _VersionField(default_factory=lambda: 3)
    field.link(version.minor, name="fake_field")
    field.value.should.equal(3)
    field.set(4)
    field.value.should.equal(4)


def test_version_comparaison():
    version = Version()
    other_version = Version()
    field = _VersionField(default_value=3)
    other_field = copy.deepcopy(field)
    field.link(version.patch, "fake_field")
    other_field.link(other_version.patch, "fake_field")
    version.should.equal(other_version)

    # making identity change shouldn't change the version
    other_field.set(3)
    version.should.equal(other_version)

    # making real change should change the version
    other_field.set(5)
    assert other_version > version
    assert version < other_version
    assert other_version.patch > version.patch
    assert version.patch < other_version.patch
    assert other_version.minor == version.minor
    assert not other_version.minor > version.minor
    assert other_version.major == version.major
    assert not other_version.major > version.major


def test_op_versioned_fields_getting_and_setting(versioned_op_cls):
    op = versioned_op_cls()
    op.should.have.property("version").being.a(Version)
    op.versioned_field.should.equal(2)
    op.versioned_field = 3
    op.versioned_field.should.equal(3)


def test_op_version_evolution(versioned_op_cls):
    op = versioned_op_cls()
    op.should.have.property("version").being.a(Version)
    old_version = copy.deepcopy(op.version)
    op.versioned_field = 4
    assert op.version > old_version
    assert op.version.major > old_version.major
    assert op.version.minor == old_version.minor
    assert op.version.patch == old_version.patch

def test_subversion_string():
    subversion = SubVersion()
    field = _VersionField(default_value=3)
    field.link(subversion, "fake_field")
    version_string = str(subversion)
    version_string = SubversionString(version_string)
    version_string.should.equal(subversion)
    field.set(5)
    assert subversion > version_string
    assert version_string < subversion


def test_full_version_parsing():
    version = Version()
    field = _VersionField(default_factory=lambda:3)
    field.link(version.major, "fake_field")
    version_string = str(version)
    version_string = Version.parse(version_string)
    version_string.should.equal(version_string)

def test_version_ripeling(versioned_op_cls, downstream_op_cls):
    up = versioned_op_cls()
    down = downstream_op_cls()
    version_1  = str(down.version)
    down = down(up)
    version_2 = str(down.version)
    up.versioned_field = 5
    version_3 = str(down.version)
    
    version_1 = Version.parse(version_1)
    version_2 = Version.parse(version_2)
    version_3 = Version.parse(version_3)

    # testing evolution on link
    assert version_2 > version_1
    assert version_2.major > version_1.major and version_2.minor > version_1.minor \
           and version_2.patch > version_1.patch
    assert version_3 > version_2
    assert version_3.major > version_2.major
    assert version_3.minor == version_2.minor
    assert version_3.patch == version_2.patch
