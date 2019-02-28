from operator import attrgetter
from typing import List
from typing import Set
from typing import Optional

from chariots.core.ops import AbstractOp
from chariots.core.metadata import Metadata
from chariots.core.dataset import DataSet
from chariots.core.versioning import Signature


class Pipeline(AbstractOp):
    """
    pipeline of operations that will perform mutliple poerations in a specific order
    """
    signature = Signature("pipeline") 

    def __init__(self, input_op: Optional[AbstractOp] = None, output_op: Optional[AbstractOp] = None):
        if input_op is not None:
            input_op.previous_op = None
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
    
    @property
    def all_ops(self) -> Set[AbstractOp]:
        if not isinstance(self.output_op, list):
            all_ops = {self.output_op}
        else:
            all_ops = set(self.output_op)
        queue = list(all_ops)
        while queue:
            op_of_interest = queue.pop()
            all_ops.add(op_of_interest)
            previous = op_of_interest.previous_op
            if previous is not None:
                if not isinstance(previous, list):
                    previous = [previous]
                queue.extend(previous)

    @classmethod
    def merge(cls, pipelines: List["Pipeline"]) -> "Pipeline":
        NotImplemented
    