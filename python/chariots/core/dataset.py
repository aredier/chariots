
from typing import Iterable
from typing import Optional

from chariots.core.metadata import Metadata

class DataSet:
    """
    class that represents a dataset
    """

    def __init__(self, data: Iterable):
        self._inner_data = data
        self._metadata: Optional[Metadata]
    
    def __next__(self):
        return next(self._inner_data)