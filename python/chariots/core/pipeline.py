from operator import attrgetter
from typing import List

from chariots.core.base_op import BaseOp
from chariots.core.metadata import Metadata
from chariots.helpers.types import DataBatch


class Pipeline(BaseOp):
    """
    pipeline of operations that will perform mutliple poerations in a specific order
    """

    def __init__(self):
        self.metadata = Metadata()

    def _call(self, data_batch: DataBatch) -> DataBatch:
        pass
    
    def chain(self, other: BaseOp, head=None):
        """
        chains anoher op to the pipeline
        """
        self.metadata.chain()

    @classmethod
    def merge(cls, pipelines: List["Pipeline"]) -> "Pipeline":
        return cls.from_metadata(Metadata().merge(map(attrgetter("metadata"), pipelines)))
    
    @classmethod
    def from_metadata(cls, metadata: Metadata):
        res = cls()
        res.metadata = metadata
        return res