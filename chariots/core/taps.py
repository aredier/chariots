from typing import List

from chariots.core.ops import AbstractOp
from chariots.helpers.types import DataBatch
from chariots.core.dataset import DataSet
from chariots.core.markers import Marker


class DataTap(AbstractOp):
    """
    A data tap represents a source of data that will be used by downstream ops
    """

    def __init__(self, iterator, markers: List[Marker]):
        self._iterator = iterator
        if not isinstance(markers, list):
            markers = [markers]
        self.markers = markers
    def perform(self):
        return DataSet.from_op(map(self._preform_single, self._iterator))
    
    def _preform_single(self, batch):
        return {self.markers[0]: batch}