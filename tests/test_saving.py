import os
import json
import uuid
from typing import IO, Text

import numpy as np

from chariots.core.saving import FileSaver
from chariots.core.saving import Savable
from chariots.core.versioning import Version


test_uuid = str(uuid.uuid1())


class SavableObject(Savable):

    def __init__(self):
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