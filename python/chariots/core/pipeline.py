from operator import attrgetter
from typing import List

from chariots.core.base_op import AbstractOp
from chariots.core.metadata import Metadata
from chariots.core.dataset import DataSet
from chariots.core.versioning import Signature


class Pipeline(AbstractOp):
    """
    pipeline of operations that will perform mutliple poerations in a specific order
    """
    signature = Signature("pipeline") 

    def __init__(self):
        self.metadata = Metadata()

    def perform(self, dataset: DataSet, target = None) -> DataSet:
        present_nodes = {}
        for previous_op, next_op in self.metadata._edges:
            if previous_op is None:
                res = next_op.perform(dataset)
            else:
                data_set = present_nodes.pop(previous_op)
                res = next_op.perform(data_set)
            present_nodes.update({next_op: res})


        return present_nodes
    
    def add(self, other: AbstractOp, head=None):
        """
        chains anoher op to the pipeline
        """
        self.metadata.chain(other)

    @classmethod
    def merge(cls, pipelines: List["Pipeline"]) -> "Pipeline":
        return cls.from_metadata(Metadata().merge(map(attrgetter("metadata"), pipelines)))
    
    def _merge_single(self, other):
        self.metadata._merge_single(other.metadata)
    
    @classmethod
    def from_metadata(cls, metadata: Metadata):
        res = cls()
        res.metadata = metadata
        return res