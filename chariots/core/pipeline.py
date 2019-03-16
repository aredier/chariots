from operator import attrgetter
from typing import List
from typing import Set
from typing import Text
from typing import Any
from typing import Optional

from chariots.core.ops import AbstractOp
from chariots.core.dataset import DataSet


class Pipeline(AbstractOp):
    """
    pipeline of operations that will perform mutliple poerations in a specific order
    """

    def __init__(self, input_op: Optional[AbstractOp] = None, output_op: Optional[AbstractOp] = None):
        if input_op is not None:
            input_op.previous_op = None
        self.input_op = input_op
        self.output_op = output_op
        if self.output_op is not None:
            self._link_to_ops

    def perform(self) -> DataSet:
        return self.output_op.perform()
    
    def __call__(self, other: "AbstractOp") -> "AbstractOp":
        if self.input_op is not None:
            self.input_op.previous_op = other
        self._link_to_ops()
        return self

    def add(self, other: AbstractOp, head=None):
        """
        chains anoher op to the pipeline
        """
        if self.input_op is None:
            self.input_op = other
            self.output_op = other
        else:
            self.output_op = other(self.input_op)
        self._link_to_ops()
    
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
        return all_ops
    
    def _link_to_ops(self):
        for op in self.all_ops:
            self._link_versions(op)

    @classmethod
    def merge(cls, pipelines: List["Pipeline"]) -> "Pipeline":
        NotImplemented
    