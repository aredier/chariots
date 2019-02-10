"""
base op of chariots
"""

from abc import ABC
from abc import abstractmethod
from typing import Optional
from functools import partial

from chariots.core.versioning import Signature
from chariots.core.dataset import DataSet
from chariots.helpers.types import DataBatch


class BaseOp(ABC):
    """
    base op of a pipeline
    the main entry point of the op is going to be the call method which will be perfomed on each data batch
    """

    signature: Optional[Signature]
    #TODO: infer this from signature
    name = "paleceholder"
    
    @abstractmethod
    def _call(self, data_batch: DataBatch) -> DataBatch:
        pass

    def perform(self, dataset: DataSet, target = None) -> DataSet:
        prev_meta = dataset.metadata
        if target and target not in prev_meta.leafs:
            raise ValueError(f"{target.name} not found in dataset")
        metadata = prev_meta.chain(self)
        return DataSet.from_op(map(partial(self._perform_single, target=target), dataset), metadata)
    
    def _perform_single(self, data: DataBatch, target):
        if target is None:
           return {self.name: self._call(data[next(iter(data))])} 
        return {self.name: self._call(data[target])}