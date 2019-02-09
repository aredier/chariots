"""
base op of chariots
"""

from abc import ABC
from abc import abstractmethod
from typing import Optional

from chariots.core.versioning import Signature
from chariots.core.dataset import DataSet
from chariots.helpers.types import DataBatch


class BaseOp(ABC):
    """
    base op of a pipeline
    the main entry point of the op is going to be the call method which will be perfomed on each data batch
    """

    signature: Optional[Signature]
    
    @abstractmethod
    def _call(self, data_batch: DataBatch) -> DataBatch:
        pass

    def perform(self, dataset: DataSet) -> DataSet:
        NotImplemented