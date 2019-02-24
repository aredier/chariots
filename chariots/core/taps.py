from typing import List

from chariots.core.base_op import AbstractOp
from chariots.core.versioning import Signature
from chariots.helpers.types import DataBatch
from chariots.core.dataset import DataSet
from chariots.core.markers import Marker

class DataTap(AbstractOp):

    signature = Signature("tap")

    def __init__(self, iterator, markers: List[Marker]):
        self._iterator = iterator
        if not isinstance(markers, list):
            markers = [markers]
        self.markers = markers
    def perform(self):
        return DataSet.from_op(map(self._preform_single, self._iterator))
    
    def _preform_single(self, batch):
        return {self.name: batch}