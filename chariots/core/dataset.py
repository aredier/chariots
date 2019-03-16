
from typing import Iterable
from typing import Optional


ORIGIN = "original"

class DataSet:
    """
    class that represents a dataset as it is produced by a pipeline or an operation
    """

    def __init__(self, data: Iterable, is_initial: bool = True):
        if is_initial:
            self._inner_data = map(self._initialize, iter(data))
        else:
            self._inner_data = iter(data)
    
    def __iter__(self):
        return self
    
    def __next__(self):
        return next(self._inner_data)
    
    def _initialize(self, data):
        """
        used if the dataset doesn't have an operation
        """
        return {ORIGIN: data}

    @classmethod
    def from_op(cls, maped_data: Iterable):
        """
        constructs a DataSet from an op
        """
        return cls(maped_data, is_initial=False)
