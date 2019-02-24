from operator import attrgetter
from typing import List

from chariots.core.ops import AbstractOp
from chariots.core.metadata import Metadata
from chariots.core.dataset import DataSet
from chariots.core.versioning import Signature


class Pipeline(AbstractOp):
    """
    pipeline of operations that will perform mutliple poerations in a specific order
    """
    signature = Signature("pipeline") 

    def __init__(self, input_op: AbstractOp = None, output_op: AbstractOp = None):
        self.input_op = input_op
        self.output_op = output_op

    def perform(self) -> DataSet:
        if self.previous_op is not None:
            self.input_op(self.previous_op)
        return self.output_op.perform()

    def add(self, other: AbstractOp, head=None):
        """
        chains anoher op to the pipeline
        """
        if self.input_op is None:
            self.input_op = other
            self.output_op = other
        else:
            self.output_op = other(self.input_op)

    @classmethod
    def merge(cls, pipelines: List["Pipeline"]) -> "Pipeline":
        NotImplemented
    