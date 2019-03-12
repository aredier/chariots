"""module that provides utilites for csv IO (saver and loader)
"""
from typing import Any
from typing import List
from typing import Mapping

import numpy as np

from chariots.core import taps
from chariots.core import markers
from chariots.core import taps

IntType = np.int32
FloatType = np.float32
ArrayAsListForMarker = Mapping[markers.Marker, List[List[Any]]]

class CSVTap(taps.DataTap):

    def __init__(self, path, marker_map: Mapping[markers.Marker, List[Text]],
                 batch_size = None, batches = 1, sep=","):
        self.path = path
        self.sep = sep
        self.batch_size = batch_size or np.inf
        self.batches = batches
        self._column_data = {
            ind_col: {
                "pos": None,
                "n"
                "dtype": None
            }
        }

    def __enter__(self):
        self._file_opened = True
        self._file_io = open(self.path, "r")
        self._analyse_header()
        self._
        return self
    
    def __exit__(self):
        self._file_io.close()

    def _anlyse_header(self):
        header = next(self._file_io)
        self._col_data = {
            for col_number, col_name in enumerate(header.split(self.sep))
        }

    def _batch_generator(self):
        if not self._file_opened:
            raise ValueError("the CSVTap should be used as context, use `with`")
        for batch_number in range(self.batches):
            res = {marker:[[]] for marker in self.markers}
            for idx_in_batch, line in enumerate(self._file_io):
                if idx_in_batch >= self.batch_size:
                    break
                self._append_line(res, line)
            yield  self._with_dtypes(res)
    
    def _append_line(self, res: ArrayAsListForMarker, line: Text) -> ArrayAsListForMarker
    
    def _with_dtypes(self, res_mapping: ArrayAsListForMarker) -> Mapping[markers.Marker, np.ndarray]
        pass
    
    def perform(self):
        return taps.DataSet.from_op(self._batch_generator())
    