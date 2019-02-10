
from typing import Iterable
from typing import Optional

from chariots.core.metadata import Metadata

class DataSet:
    """
    class that represents a dataset
    """

    def __init__(self, data: Iterable, is_initial: bool = True):
        if is_initial:
            self._inner_data = map(self._initialize, iter(data))
        else:
            self._inner_data = iter(data)
        self.metadata = Metadata()

    def __iter__(self):
        return self
    
    def __next__(self):
        return next(self._inner_data)
    
    def _initialize(self, data):
        return {"source": data}

    @classmethod
    def from_op(cls, maped_data: Iterable, metadata: Metadata):
        res = cls(maped_data, is_initial=False)
        res.metadata = metadata
        return res