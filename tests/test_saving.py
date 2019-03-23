# pylint: disable=no-member
import os
import json
import uuid
from typing import IO, Text

import numpy as np

from chariots.core.saving import FileSaver
from chariots.core.ops import BaseOp
from chariots.core.requirements import Number
from chariots.core.saving import Savable
from chariots.core.versioning import Version, VersionField, SubVersionType, VersionType


test_uuid = str(uuid.uuid1())


class SavableObject(Savable, BaseOp):

    all_versioned_field = VersionField(SubVersionType.MINOR,
                                       target_version=VersionType.ALL,
                                       default_value=int(np.random.randint(1000)))
    runtime_versioned_field = VersionField(SubVersionType.MAJOR,
                                           target_version=VersionType.RUNTIME,
                                           default_value=int(np.random.randint(1000)))
    saving_versioned_field = VersionField(SubVersionType.MAJOR,
                                          target_version=VersionType.SAVING,
                                          default_value=int(np.random.randint(1000)))

    def _main(self) -> Number:
        raise ValueError("not executable")

    def __init__(self):
        self.unique = int(np.random.randint(1000))
        self.seed = int(np.random.randint(1000))

    def _serialize(self, temp_dir: Text):
        with open(os.path.join(temp_dir, "model.json"), "w") as file:
            json.dump(self.seed, file)

    @classmethod
    def _deserialize(cls, temp_dir: Text) -> "Savable":
        res = cls()
        with open(os.path.join(temp_dir, "model.json"), "r") as file:
            res.seed = json.load(file)
        return res

    @classmethod
    def checksum(cls):
        saving_version, _ = cls._build_version()
        return saving_version

    @classmethod
    def identifiers(cls):
        return {"type": "test", "instance": test_uuid}


def test_saving(saver):
    foo =  SavableObject()
    foo.save(saver)
    bar = SavableObject.load(saver)
    foo.seed.should.be.equal(bar.seed)
    foo.all_versioned_field.should.be.equal(bar.all_versioned_field)
    foo.runtime_versioned_field.should.be.equal(bar.runtime_versioned_field)
    foo.saving_versioned_field.should.be.equal(bar.saving_versioned_field)
    foo.unique.should_not.be.equal(bar.seed)


def test_loading_deprecated(saver):
    foo =  SavableObject()
    foo.save(saver)
    SavableObject.saving_versioned_field.default_value = -1
    SavableObject.load.when.called_with(saver).should.throw(ValueError)


def test_runtime_deprecated(saver):
    foo =  SavableObject()
    foo.save(saver)
    foo_version = str(foo.runtime_version)
    SavableObject.runtime_versioned_field.default_value = -1
    bar = SavableObject.load(saver)
    assert bar.runtime_version.major > Version.parse(foo_version).major
