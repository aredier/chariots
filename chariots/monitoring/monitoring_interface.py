from typing import Mapping, Type, Any, Text, Optional, List
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


    table_name: Text = None

    # you can add MonitoringFields as class argguments

    @classmethod
    def get_fields_dict(cls):
        return {attr_name: attr for attr_name, attr in cls.__dict__.items()
                if isinstance(attr, MonitoringField)}

    def dump_data(self, **kwargs):
        pass

    def flush(self):
        pass

class MonitoringField:

    def __init__(self, dtype: FieldTypes, default_value: Optional[Any] = None, optional: bool = True,
                 grouping_behavior: Optional[Mapping[MonitoringField,FieldGroupingBehavior]] = None,
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
        self._all_tables_table = TablesMonitoringTable()
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
    def dump_data(self, table_name, fields_map: Mapping[Text, Any]):
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, *args, **kwargs):
        pass

