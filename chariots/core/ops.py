"""
base op of chariots
"""

import random
from abc import ABC
from abc import abstractmethod
from abc import abstractclassmethod
from abc import ABCMeta
from typing import Optional
from typing import List
from typing import Mapping
from functools import partial

from chariots.core.versioning import Signature
from chariots.core.dataset import DataSet, ORIGIN
from chariots.core.markers import Marker
from chariots.helpers.types import DataBatch
from chariots.helpers.types import Requirements
from chariots.helpers.utils import SplitPuller
from chariots.helpers.utils import SplitPusher


class AbstractOp(ABC):
    """
    base op of a pipeline
    the main entry point of the op is going to be the perform method.
    there are several fields that are needed to create an op:
        - marker : corresponds to the markers of this op, these will be searched by the next op in the pipeline as parameters for their _main method
        - signature: corresponds to the version of this op (this is what will be used to determine if ops are compatible)
    """

    signature: Signature = None
    previous_op = None
    markers: List[Marker] = []
    requires: Requirements = {}

    def __new__(cls, *args, **kwargs):
        """
        checks that fields are implemented
        """
        if cls.signature is None:
            raise ValueError(f"no signature was assigned to {cls.__name__}")
        instance = super(AbstractOp, cls).__new__(cls)
        # instance.signature.add_fields(random_identifier = str(random.random() // 1e-16))
        return instance
    
    def __call__(self, other: "AbstractOp") -> "AbstractOp":
        """
        used to determine the ancestor of an op
        """
        if not isinstance(other, AbstractOp):
            raise ValueError("call does only work with single ops. if you want another behavior, override the __Call__ method") 
        self._check_compatibility(other, self.requires)
        self.previous_op = other
        return self
    
    @staticmethod
    def _check_compatibility(other: "AbstractOp", requirements: Requirements):
        missing = next((required for required in requirements.items()
                        if all(not required[1].compatible(marker) for marker in other.markers)),
                       None)
        if missing is not None:
            raise ValueError(f"requirement {missing} not fulfiled by {other.name}")

    @property
    def name(self):
        """
        the name of the op
        """
        return self.signature.name
    
    @abstractmethod
    def perform(self) -> "DataSet":
        """
        the main entry point of an op that should perform the op's ancestors and th op itself and 
        returns the resulting DataSet
        """
        pass
    
    @property
    def ready(self):
        if self.previous_op:
            return self.previous_op.ready
        return True
    

class BaseOp(AbstractOp):
    """
    BaseOp is a simple implementation of an op were _main is performed on each data batch individually
    in order to do that, a litle magic (not too much I hope) is added to determine wich part of the 
    data batch should become which argument of the _main method:
    The key of each requirement is used as the parameter_name of the corresponding argument in _main
    hence all the arguments of _main must be keys of the required dict
    """

    @abstractmethod
    def _main(self, **kwargs) -> DataBatch:
        """
        function to be overriden to create an op, the kwargs dict will have the same keys as the 
        requirements and the values will be the part of the batch corresponding to the marker.
        """

    def perform(self) -> "DataSet":
        """
        implementation of perform for the base op
        """
        if self.previous_op is None:
            raise ValueError("this pipeline doesn't seem to have a tap, can't get the data flowing")
        return self._map_op(self.previous_op.perform())
    
    def _map_op(self, data_set: DataSet):
        """
        maps itself to a dataset
        """
        return DataSet.from_op(map(self._perform_single, data_set))
    
    def _perform_single(self, data: DataBatch):
        """
        performs the argument resolution executes the op on a databatch
        """
        args_dict = self._resolve_arguments(data, self.requires)
        res = self._main(**args_dict)
        return dict(zip(self.markers, res if isinstance(res, tuple) else (res,)))
    
    def _resolve_arguments(self, data: dict, requirements: Requirements):
        return {arg_name: next(data_batch for data_marker, data_batch in data.items() if marker.compatible(data_marker))
                for arg_name, marker in requirements.items()}

    
class Split(AbstractOp):
    """
    split operation that creates several downstreams from a single upstream
    be carefull, splits are not free as they have to do a deepcopy of each batch to prevent data races
    """

    signature = Signature(name = "split")

    def __init__(self, n_splits: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._n_splits = n_splits
    
    @property
    def markers(self):
        return self.previous_op.markers
    
    def __call__(self, other: "AbstractOp") -> List["_SplitRes"]:
        self.previous_op = other
        self._pusher = SplitPusher(self._n_splits)
        return [_SplitRes(puller, self) for puller in self._pusher.pullers]
    
    def perform(self):
        if self.previous_op is None:
            raise ValueError("this pipeline doesn't seem to have a tap, can't get the data flowing")
        self._pusher.set_iterator(self.previous_op.perform())

    
class _SplitRes(AbstractOp):
    """
    downstream op of a split (returned by Split.__call__)
    """

    signature = Signature(name = "split_puller")

    def __init__(self, puller: SplitPuller, split_op: Split, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._puller = puller
        self.previous_op = split_op

    @property
    def markers(self):
        return self.previous_op.markers
    
    def __call__(self, other: "AbstractOp") -> "AbstractOp":
        raise ValueError("split puller should not be called directly")
    
    def perform(self) -> DataSet:
        self.previous_op.perform()
        return DataSet.from_op(self._puller)

class Merge(AbstractOp):
    """
    Op that merges sevreral pipelines in a single one
    """

    signature = Signature(name = "merge")

    def __init__(self, *args, **kwargs):
        self.previous_op = None
        super().__init__(*args, **kwargs)

    def perform(self) -> DataSet:
        ziped = zip(*(op.perform() for op in self.previous_op))
        return map(self._perform_single, ziped)

    @property
    def markers(self):
        return [marker for op in self.previous_op for marker in op.markers]
    
    def _perform_single(self, ziped):
        res = {}
        for partial in ziped:
            res.update(partial)
        return res
    
    @property
    def ready(self):
        return all([op.ready for op in self.previous_op])

    def __call__(self, other: List["AbstractOp"]) -> "AbstractOp":
        self.previous_op = other
        return self

