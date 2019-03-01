import json
from typing import IO

import numpy as np

from chariots.core.saving import FileSaver
from chariots.core.saving import Savable

class SavableObject(Savable):

    def __init__(self):
        self.seed = int(np.random.randint(10))
    
    def _serialize(self, temp_file: IO):
        temp_file.write(json.dumps(self.seed).encode())        

    @classmethod
    def _deserialize(cls, file: IO) -> "Foo":
        res = cls()
        res.seed = json.load(file)
        return res
    
    @classmethod
    def checksum(self):
        return "foobar"

def test_saving():
    foo =  SavableObject()
    saver = FileSaver()
    foo.save(saver)
    bar = SavableObject.load(saver)
    foo.seed.should.be.equal(bar.seed)