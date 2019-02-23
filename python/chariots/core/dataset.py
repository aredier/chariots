
from typing import Iterable
from typing import Optional

from chariots.core.metadata import Metadata

ORIGIN = "original"

class DataSet:
    """
    class that represents a dataset
    """

    def __init__(self, data: Iterable, is_initial: bool = True):
        if is_initial:
            self._inner_data = map(self._initialize, iter(data))
        else:
            self._inner_data = iter(data)
    
    def merge(self, other: "DataSet"):
        return self.from_op(zip(self, other))

    def __iter__(self):
        return self
    
    def __next__(self):
        return next(self._inner_data)
    
    def _initialize(self, data):
        return {ORIGIN: data}

    @classmethod
    def from_op(cls, maped_data: Iterable):
        return cls(maped_data, is_initial=False)
