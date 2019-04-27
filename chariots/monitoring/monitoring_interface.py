from datetime import datetime
from typing import Mapping, Type, Any, Text, Optional, List, Dict
from abc import ABC, abstractmethod
from enum import Enum

from influxdb import InfluxDBClient
from sqlalchemy import Column, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


INFLUX_DB_NAME = "chariots_monitoring"
SQL_BASE = declarative_base()


class SeriesNumericalDisplayFormat(Enum):
    NONE = -1
    TABLE = 1


class SeriesGraphicalDisplayFormat(Enum):
    NONE = -1
    LINE_CHART = 1


class FieldGroupingBehavior(Enum):
    MAX = 1
    MIN = 2
    AVERAGE = 2


class FieldTypes(Enum):
    INT = 0
    FLOAT = 1
    TEXT = 2


class MonitoringField:

    def __init__(self, dtype: FieldTypes, default_value: Optional[Any] = None, optional: bool = True,
                 grouping_behavior: FieldGroupingBehavior = None,
                 ):
        self.dtype = dtype
        self.grouping_behavior = grouping_behavior
        self.is_optional = optional
        self.default_value = default_value

    @property
    def metadata(self):
        return {
            "dtype": self.dtype.value,
            "groupings": {field: grouping.value for field, grouping in
                          self.grouping_behavior.items()},
            "optional": self.is_optional,
            "default": self.default_value,
        }


class MonitoringSeries(ABC):
    series_name = None

    numerical_display_format: SeriesNumericalDisplayFormat = SeriesNumericalDisplayFormat.TABLE
    graphical_display_format: SeriesGraphicalDisplayFormat = SeriesGraphicalDisplayFormat.LINE_CHART

    #you can add fields bellow

    def __init__(self, interface: "MonitoringInterface"):
        self._interface = interface
        self._interface.register_series(self)

    def dump_data(self, **kwargs):
        self._interface.register_data(self, self._check_data_validity_and_fill_defaults(kwargs))

    def _check_data_validity_and_fill_defaults(self, data: Mapping[Text, Any]):
        fields_dict = self.get_fields_dict()
        res = {}
        for key, value in data.items():
            if not fields_dict.pop(key, None):
                raise ValueError(f"field is not present in table {self.series_name}: {key}")
            res[key] = value
        if not all([field.is_optional for field in fields_dict.values()]):
            raise ValueError("non optional field is None")
        res.update({field_name: field.default_value for field_name, field in fields_dict.items()})
        return res

    @classmethod
    def get_fields_dict(cls) -> Mapping[Text, MonitoringField]:
        return {attr_name: attr for attr_name, attr in cls.__dict__.items()
                if isinstance(attr, MonitoringField)}


class MonitoringSeriesMetadata(SQL_BASE):
    __tablename__ = "series_metadata"

    id = Column(Integer, primary_key=True)
    series_name = Column(String, nullable=False)
    graphical_display = Column(Integer, default=SeriesGraphicalDisplayFormat.NONE.value)
    numerical_display = Column(Integer, default=SeriesNumericalDisplayFormat.NONE.value)

    def __str__(self):
        return f"<SeriesMetada of {self.series_name}, graphical: {self.graphical_display}, " \
            f"numerical: {self.numerical_display}>"


class MonitoringInterface:

    def __init__(self, sql_engine: Engine, influx_client: InfluxDBClient,
                 influx_db_name: Text = INFLUX_DB_NAME):
        # initializing sql
        self._sql_engine = sql_engine
        self._sql_session_maker = sessionmaker(bind=self._sql_engine)

        # initializing influx
        self._influx_db_name = influx_db_name
        self._influx_client = influx_client
        self._influx_client.create_database(self._influx_db_name)

        self._uncreated_series = []
        self._entered = False

    def register_series(self, series: MonitoringSeries):
        if self._entered:
            self._add_series_to_db(series)
        else:
            self._uncreated_series.append(series)

    def register_data(self, series: MonitoringSeries, data: Mapping):
        self._influx_client.write_points([{
            "measurement": series.series_name,
            "time": datetime.utcnow().isoformat(),
            "fields": data
        }])
        print("foo")

    def _add_series_to_db(self, series: MonitoringSeries):
        session = self._sql_session_maker()
        session.add(self._create_table_instance(series))
        session.commit()

    def _create_table_instance(self, series: MonitoringSeries):
        return MonitoringSeriesMetadata(series_name=series.series_name,
                                        graphical_display=series.graphical_display_format.value,
                                        numerical_display=series.numerical_display_format.value)

    def __enter__(self):
        SQL_BASE.metadata.create_all(self._sql_engine)

        for series in self._uncreated_series:
            self._add_series_to_db(series)
        self._entered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


