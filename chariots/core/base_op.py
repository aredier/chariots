"""
base op of chariots
"""

import random
from abc import ABC
from abc import abstractmethod
from abc import ABCMeta
from typing import Optional
from typing import List
from functools import partial

from chariots.core.versioning import Signature
from chariots.core.dataset import DataSet, ORIGIN
from chariots.helpers.types import DataBatch
from chariots.helpers.utils import SplitPuller
from chariots.helpers.utils import SplitPusher


class AbstractOp(ABC):
    """
    base op of a pipeline
    the main entry point of the op is going to be the call method which will be perfomed on each data batch
    """

    signature: Signature = None
    previous_op = None

    def __new__(cls, *args, **kwargs):
        if cls.signature is None:
            raise ValueError(f"no signature was assigned to {cls.__name__}")
        instance = super(AbstractOp, cls).__new__(cls)
        # instance.signature.add_fields(random_identifier = str(random.random() // 1e-16))
        return instance
    
    def __call__(self, other: "AbstractOp") -> "AbstractOp":
        if not isinstance(other, AbstractOp):
            raise ValueError("call does only work with single ops. if you want another behavior, override the __Call__ method") 
        self.previous_op = other
        return self
        

    @property
    def name(self):
        return self.signature.name
    
    @abstractmethod
    def perform(self) -> "DataSet":
        pass
    

class BaseOp(AbstractOp):

    @abstractmethod
    def _main(self, **kwargs) -> DataBatch:
        pass

    def perform(self) -> "DataSet":
        if self.previous_op is None:
            raise ValueError("this pipeline doesn't seem to have a tap, can't get the data flowing")
        return self._map_op(self.previous_op.perform())
    
    def _map_op(self, data_set: DataSet):
        return DataSet.from_op(map(self._perform_single, data_set))
    
    def _perform_single(self, data: DataBatch):
        return {self.name: self._main(**data)}
    
class Split(AbstractOp):

    signature = Signature(name = "split")

    def __init__(self, n_splits: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._n_splits = n_splits
    
    def __call__(self, other: "AbstractOp") -> List["_SplitRes"]:
        self.previous_op = other
        self._pusher = SplitPusher(self._n_splits)
        return [_SplitRes(puller, self) for puller in self._pusher.pullers]
    
    def perform(self):
        if self.previous_op is None:
            raise ValueError("this pipeline doesn't seem to have a tap, can't get the data flowing")
        self._pusher.set_iterator(self.previous_op.perform())

    
class _SplitRes(AbstractOp):

    signature = Signature(name = "split_puller")

    def __init__(self, puller: SplitPuller, split_op: Split, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._puller = puller
        self.previous_op = split_op
    
    def __call__(self, other: "AbstractOp") -> "AbstractOp":
        raise ValueError("split puller should not be called directly")
    
    def perform(self) -> DataSet:
        self.previous_op.perform()
        return DataSet.from_op(self._puller)

class Merge(AbstractOp):

    signature = Signature(name = "merge")

    def __init__(self, *args, **kwargs):
        self.merged_ops = None
        super().__init__(*args, **kwargs)

    def perform(self) -> DataSet:
        ziped = zip(*(op.perform() for op in self.merged_ops))
        return map(self._perform_single, ziped)
    
    def _perform_single(self, ziped):
        res = {}
        for partial in ziped:
            res.update(partial)
        return res

    def __call__(self, other: List["AbstractOp"]) -> "AbstractOp":
        self.merged_ops = other
        return self
    