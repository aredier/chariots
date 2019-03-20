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
from chariots.core.versioning import Version, VersionField, SubVersionType


test_uuid = str(uuid.uuid1())


class SavableObject(Savable, BaseOp):

    foo = VersionField(SubVersionType.MINOR, default_value=3)
    
    def _main(self) -> Number:
        raise ValueError("not executable")

    def __init__(self):
        self.unique = int(np.random.randint(1000))
        self.seed = int(np.random.randint(1000))
        self.foo = int(np.random.randint(1000))
    
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
    def checksum(self):
        return Version()

    @classmethod
    def identifiers(cls):
        return {"type": "test", "instance": test_uuid}

def test_saving():
    foo =  SavableObject()
    saver = FileSaver()
    foo.save(saver)
    bar = SavableObject.load(saver)
    foo.seed.should.be.equal(bar.seed)
    foo.foo.should.be.equal(bar.foo)
    foo.unique.should_not.be.equal(bar.seed)