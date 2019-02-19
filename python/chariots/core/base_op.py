"""
base op of chariots
"""

import random
from abc import ABC
from abc import abstractmethod
from abc import ABCMeta
from typing import Optional
from functools import partial

from chariots.core.versioning import Signature
from chariots.core.dataset import DataSet
from chariots.helpers.types import DataBatch


class AbstractOp(ABC):
    """
    base op of a pipeline
    the main entry point of the op is going to be the call method which will be perfomed on each data batch
    """

    signature: Signature = None

    def __new__(cls, *args, **kwargs):
        if cls.signature is None:
            raise ValueError(f"no signature was assigned to {cls.__name__}")
        instance = super(AbstractOp, cls).__new__(cls, *args, **kwargs)
        # instance.signature.add_fields(random_identifier = str(random.random() // 1e-16))
        return instance

    @property
    def name(self):
        return self.signature.name
    
    @abstractmethod
    def perform(self, dataset: DataSet, target = None) -> DataSet:
        pass
    

class BaseOp(AbstractOp):

    @abstractmethod
    def _call(self, data_batch: DataBatch) -> DataBatch:
        pass

    def perform(self, dataset: DataSet, targets = None) -> DataSet:
        prev_meta = dataset.metadata
        targets = targets or prev_meta.leafs
        metadata = prev_meta.chain(self)
        return DataSet.from_op(map(partial(self._perform_single, targets=targets), dataset), prev_meta)
    
    def _perform_single(self, data: DataBatch, targets):
        if len(targets) > 1:
            print("foo")
            return {self.name: self._call({key: value for key, value in data if key in targets})}
        print
        return {self.name: self._call(data[next(iter(data))])}