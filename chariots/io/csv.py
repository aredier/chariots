"""module that provides utilites for csv IO (saver and loader)
"""
import operator
from typing import Any
from typing import List
from typing import Mapping
from typing import Text

import numpy as np

from chariots.core import taps
from chariots.core import markers
from chariots.core import taps

IntType = np.int32
FloatType = np.float32
ArrayAsListForMarker = Mapping[markers.Marker, List[List[Any]]]

class CSVTap(taps.DataTap):

    def __init__(self, path, name_for_marker: Mapping[markers.Marker, List[Text]],
                 batch_size = None, batches = None, sep=","):
        self.path = path
        self.sep = sep
        self.batch_size = batch_size or np.inf
        self.batches = batches
        self._name_for_marker = name_for_marker
        self._dtype_for_marker = {}

    def __enter__(self):
        self._file_opened = True
        self._file_io = open(self.path, "r")
        self._analyse_header()
        return self
    
    def __exit__(self, *args):
        self._file_io.close()

    def _analyse_header(self):
        header = next(self._file_io)
        header_names = header.strip().split(self.sep)
        self._col_for_marker = {
            marker: list(map(header_names.index, names))
            for marker, names in self._name_for_marker.items() 
        }
        self.markers = list(self._col_for_marker.keys())

    def _batch_generator(self):
        if not self._file_opened:
            raise ValueError("the CSVTap should be used as context, use `with`")
        res = {marker:[] for marker in self.markers}
        for idx, line in enumerate(self._file_io):
            if self.batches is not None and idx >= self.batch_size * self.batches:
                break
            if idx and not idx % self.batch_size:
                yield  self._with_dtypes(res)
                res = {marker:[] for marker in self.markers}
            self._append_line(res, line)
        yield  self._with_dtypes(res)
    
    def _append_line(self, res: ArrayAsListForMarker, line: Text) -> ArrayAsListForMarker:
        values = line.strip().split(self.sep)
        for marker, colums in self._col_for_marker.items():
            matrix_row = [values[i] for i in colums]
            res[marker].append(matrix_row)
        return res

    def _with_dtypes(self, res_mapping: ArrayAsListForMarker) -> Mapping[markers.Marker, np.ndarray]:
        if self._dtype_for_marker:
            return {
                marker: np.array(res_mapping[marker], dtype)
                for marker, dtype in self._dtype_for_marker.items()
            }
        final = {}
        for marker, array_as_list in  res_mapping.items():
            try:
                array = np.array(array_as_list, IntType)
                self._dtype_for_marker[marker] = IntType
                final[marker] = array
                continue
            except ValueError:
                # casting to int failed, trying something else
                pass
            try:
                array = np.array(array_as_list, IntType)
                self._dtype_for_marker[marker] = FloatType
                final[marker] = array
                continue
            except ValueError:
                # casting to int failed, trying something else
                pass
            array = np.array(array_as_list)
            final[marker] = array
            self._dtype_for_marker[marker] = np.dtype("<U3")
        return final



    
    def perform(self):
        return taps.DataSet.from_op(self._batch_generator())
    