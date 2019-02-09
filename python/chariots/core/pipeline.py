
from chariots.core.base_op import BaseOp

class Pipeline(BaseOp):
    """
    pipeline of operations that will perform mutliple poerations in a specific order
    """

    def __init__(self):
        self.metadata = MetaData()

    def _call(self, data_batch: DataBatch) -> DataBatch:
        pass
    
    def chain(self, other: BaseOp, head=None):
        """
        chains anoher op to the pipeline
        """
        self.metadata.chain()

    @classmethod
    def merge(cls, pipelines: List["Pipeline"]) -> Pipeline:
        return cls.from_metadata(Metadata().merge(map(, pipelines)))