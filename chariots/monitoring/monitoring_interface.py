import operator
import os
import time
from typing import Mapping, Type, Any, Text, Optional, List, Dict
from abc import ABC, abstractmethod
from enum import Enum

class FieldTypes(Enum):
    INT = 0
    FLOAT = 1
    TEXT = 2


class TableNumericalDisplayFormat(Enum):
    NONE = -1
    TABLE = 1


class TableGraphicalDisplayFormat(Enum):
    NONE = -1
    LINE_CHART = 1


class AbstractMonitoringTable(ABC):
    """
    in memory buffer that defines and store temporarly (to reduce io operations)
    the data for a monitoring table
    """


    numerical_display_format: TableNumericalDisplayFormat = TableNumericalDisplayFormat.TABLE
    graphical_display_format: TableGraphicalDisplayFormat = TableGraphicalDisplayFormat.LINE_CHART


    able_name: Text = None

    # frequency at which the flush should happen
    flush_time_sep = 1

    # you can add MonitoringFields as class argguments

    def __init__(self, interface: "AbstractMonitoringInterface", _register=True):
        self._last_flush = time.time()
        self._cache = []
        self._interface = interface
        if _register:
            self._interface.register_table(self)

    @classmethod
    def get_fields_dict(cls) -> Dict[Text, "MonitoringField"]:
        return {attr_name: attr for attr_name, attr
                in sorted(list(cls.__dict__.items()), key=operator.itemgetter(0))
                if isinstance(attr, MonitoringField)}

    def dump_data(self, **kwargs):
        self._check_data_validity(kwargs)
        self._cache.append(kwargs)
        present_time = time.time()
        if present_time - self._last_flush > self.flush_time_sep:
            self.flush()
            self._last_flush = present_time

    def _check_data_validity(self, data: Mapping[Text, Any]):
        fields_dict = self.get_fields_dict()
        for key, value in data.items():
            if not fields_dict.pop(key, None):
                raise ValueError(f"field is not present in table {self.table_name}: {key}")
        if not all([field.is_optional for field in fields_dict.values()]):
            raise ValueError("non optional field is None")

    def flush(self):
        print("flushing", self.table_name)
        print(self._cache)
        self._interface.dump_data(self, self._cache)
        self._cache = []

class MonitoringField:

    def __init__(self, dtype: FieldTypes, default_value: Optional[Any] = None, optional: bool = True,
                 grouping_behavior: Optional[Mapping["MonitoringField", "FieldGroupingBehavior"]] = None,
                 ):
        self.dtype = dtype
        self.grouping_behavior = grouping_behavior or {}
        self.is_optional = optional
        self.default_value = default_value


class FieldGroupingBehavior(Enum):
    MAX = 1
    MIN = 2
    AVERAGE = 2


class MonitoringFieldGrouping:
    def __init__(self, possible_grouping_behavior: Optional[List[FieldGroupingBehavior]],
                 is_forget_grouping: bool = False):
        pass

class TablesMonitoringTable(AbstractMonitoringTable):
    """a table to monitor all the other tables present in the monitoring system"""

    numerical_display_format = TableNumericalDisplayFormat.NONE
    graphical_display_format = TableGraphicalDisplayFormat.NONE
    table_name = "_all_fields"

    _numerical_display_format = MonitoringField(dtype=FieldTypes.INT, default_value=None,
                                                optional=False)
    _graphical_display_format = MonitoringField(dtype=FieldTypes.INT, default_value=None,
                                                optional=False)

    _table_name = MonitoringField(dtype=FieldTypes.TEXT, default_value=None,
                                                optional=False)


class AbstractMonitoringInterface(ABC):

    def __init__(self):
        self._all_tables_table = TablesMonitoringTable(self, _register=False)
        self._initialise_table(TablesMonitoringTable)

    def register_table(self, table: AbstractMonitoringTable):
        self._all_tables_table.dump_data(_numerical_display_format=table.numerical_display_format,
                                         _graphical_display_format=table.graphical_display_format,
                                         _table_name=table.table_name)
        self._all_tables_table.flush()
        self._initialise_table(table)

    @abstractmethod
    def _initialise_table(self, table: "AbstractMonitoringTable"):
        pass

    @abstractmethod
    def dump_data(self, table_name, fields_maps: List[Mapping[Text, Any]]):
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, *args, **kwargs):
        pass

class CSVMonitoringInterface(AbstractMonitoringInterface):

    def __init__(self, path: Text, *args, **kwargs):
        self._dir_path = path
        self._needed_file_map = []
        self._file_map: Optional[dict] = None
        super().__init__(*args, **kwargs)

    def _initialise_table(self, table: AbstractMonitoringTable):
        if self._file_map is None:
            self._needed_file_map.append({"t_name": table.table_name, "t_fields": table.get_fields_dict()})
        else:
            self._file_map[table.table_name] = open(self._build_name(table.table_name), "a")
            self._initialise_table_header(table.table_name, table.get_fields_dict())

    def _initialise_table_header(self, table_name, fields_dict):
        self._file_map[table_name].write(self._format_line(fields_dict.keys()))
        self._file_map[table_name].flush()

    def _format_line(self, line_elements):
        print("---------------------", line_elements)
        return ",".join(map(str, line_elements)) + "\n"

    def _build_name(self, table_name):
        return os.path.join(self._dir_path, table_name + ".csv")

    def dump_data(self, table: AbstractMonitoringTable, fields_maps: List[Mapping[MonitoringFieldGrouping, Any]]):
        for field_map in fields_maps:
            line_elements = []
            for field_name, field in table.get_fields_dict().items():
                potential = field_map.get(field_name, None)
                if potential is None and field.is_optional:
                    potential = field.default_value
                if potential is None:
                    raise ValueError("non optional field is None")
                line_elements.append(potential)
            self._file_map[table.table_name].write(self._format_line(line_elements))

    def __enter__(self):
        os.makedirs(self._dir_path, exist_ok=True)
        self._file_map = {}
        for table_data in self._needed_file_map:
            t_name = table_data["t_name"]
            self._file_map[t_name] = open(self._build_name(t_name), "a")
            self._initialise_table_header(t_name, table_data["t_fields"])
        return self

    def __exit__(self, *args, **kwargs):
        for file in self._file_map.values():
            file.close()
        self._file_map = None

