from datetime import datetime
from typing import Mapping, Type, Any, Text, Optional, List, Dict, Tuple
from abc import ABC, abstractmethod
from enum import Enum

from influxdb import InfluxDBClient
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from chariots.core.versioning import Version
from chariots.core.ops import _AbstractOp


INFLUX_DB_NAME = "chariots_monitoring"
SQL_BASE = declarative_base()


def create_default_dbs() -> Tuple[Engine, InfluxDBClient]:
    """
    creates default connexions to the db in case the user doesn't provide any
    :return: the sqlqlchemy engine and the influxdb client
    """
    engine = create_engine('sqlite:////tmp/chariots.db', convert_unicode=True, echo=True)
    client = InfluxDBClient('localhost', 8086, 'root', 'root', INFLUX_DB_NAME)
    return engine, client


class SeriesNumericalDisplayFormat(Enum):
    """
    the accepted forms of numerical display for the front end
    """
    NONE = -1
    TABLE = 1


class SeriesGraphicalDisplayFormat(Enum):
    """
    the accepted graphical display formats for the front end
    """
    NONE = -1
    LINE_CHART = 1


class FieldGroupingBehavior(Enum):
    """
    thhe field grouping behavior
    this is curently not implemented but will be used to create the landing page of the UI in the future
    """
    MAX = 1
    MIN = 2
    AVERAGE = 2


class FieldTypes(Enum):
    """
    the accepted field types
    """
    INT = 0
    FLOAT = 1
    TEXT = 2


class MonitoringField:
    """
    the basis of a field in a series

    :param dtype: the FieldType associated to the field (TODO this is currently not used)
    :param default_value: the default value to use when the field is not specified
    :param optional: whether the field is optional or not
    :param grouping_behavior: the grouping behavior whem summarising this filed (TODO This is currently not used)
 )    """

    def __init__(self, dtype: FieldTypes, default_value: Optional[Any] = None, optional: bool = True,
                 grouping_behavior: FieldGroupingBehavior = None):
        """
        :param dtype: the FieldType associated to the field. THIS IS CURRENTLY NOT USED
        :param default_value: the default value to use when the field is not specified
        :param optional: whether the field is optional or not
        :param grouping_behavior: the grouping behavior whem summarising this filed THIS IS CURRENTLY NOT USED
        """
        self.dtype = dtype
        self.grouping_behavior = grouping_behavior
        self.is_optional = optional
        self.default_value = default_value

    @property
    def metadata(self) -> Mapping[Text, Any]:
        """
        returns the metadata of the field
        :return: the json of the metadata
        """
        return {
            "dtype": self.dtype.value,
            "groupings": {field: grouping.value for field, grouping in
                          self.grouping_behavior.items()},
            "optional": self.is_optional,
            "default": self.default_value,
        }


class MonitoringSeries(ABC):
    """
    class representing an entire series for the monitoring framework.
    it is constituted of several fields (class attributes)
    The class is instantiated by providing an interface (representing DB connexions and doing integrity checks).
    The main entry point is the `dump_data` method

    :param interface: the interface to link this series to.
    """

    series_name = None

    # how to display this series full data in the front end (TODO this currently has not effect)
    numerical_display_format: SeriesNumericalDisplayFormat = SeriesNumericalDisplayFormat.TABLE
    # how to represent the data (line charts and what not) in the front end (TODO this currently has not effect)
    graphical_display_format: SeriesGraphicalDisplayFormat = SeriesGraphicalDisplayFormat.LINE_CHART

    def __init__(self, interface: "MonitoringInterface"):
        """
        :param interface: the interface to link this series to.
        """
        self._interface = interface
        self._interface.register_series(self)

    def dump_data(self, version: Optional[Version] = None, **kwargs):
        """
        dumps data through this series' interface

        :param version: the version associated to this data point if applicable
        :param kwargs: the data of the fields
        """

        self._interface.register_data(self, self._check_data_validity_and_fill_defaults(kwargs), version)

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
        """
        returns a dict with the fields of the series in keys and the fields themselves in value

        :return: the fields diuct
        """

        return {attr_name: attr for attr_name, attr in cls.__dict__.items()
                if isinstance(attr, MonitoringField)}


class _MonitoringSeriesMetadata(SQL_BASE):
    """
    the metadata of the monitoring series. This uses an SQL table in order not to replicate the data for each point of
    the series (in influx) and ensure consistency
    """
    __tablename__ = "series_metadata"

    id = Column(Integer, primary_key=True)
    series_name = Column(String, nullable=False)
    graphical_display = Column(Integer, default=SeriesGraphicalDisplayFormat.NONE.value)
    numerical_display = Column(Integer, default=SeriesNumericalDisplayFormat.NONE.value)

    def __str__(self):
        return f"<SeriesMetada of {self.series_name}, graphical: {self.graphical_display}, " \
            f"numerical: {self.numerical_display}>"


class DBVersion(SQL_BASE):
    """
    a collection of all the version that have been fed through the monitoring framework. This is used not to have run
    through all the data in order to find all the versions
    """
    __tablename__ = "version"

    id = Column(Integer, primary_key=True)
    major_checksum = Column(String, nullable=False)
    minor_checksum = Column(String, nullable=False)
    patch_checksum = Column(String, nullable=False)

    def __str__(self):
        return f"<DBVersion {self.major_checksum}.{self.minor_checksum}.{self.patch_checksum}"

    @classmethod
    def from_version(cls, version: Version) -> "DBVersion":
        """
        converts a Chariot version into the db format

        :param version: the version
        :return: a DB savable object representing the input
        """

        return cls(
            major_checksum=version.major.fields_hash,
            minor_checksum=version.minor.fields_hash,
            patch_checksum=version.patch.fields_hash
        )

    @classmethod
    def find_equivalent_filter_builder(cls, version: Version) -> Tuple:
        """
        creates a filters to find the same version as a specific chariots version

        :param version:
        :return:
        """
        return (
            cls.major_checksum == version.major.fields_hash,
            cls.minor_checksum == version.minor.fields_hash,
            cls.patch_checksum == version.patch.fields_hash
        )


class MonitoringInterface:
    """
    context manager to use when monitoring
    abstractions of the DBs (and potentially other persistence format in the future)
    this is the entry point for a series to dump its data

    :param sql_engine: the sql engine to save metadata and versions
    :param influx_client: the influx client to save the data to
    :param influx_db_name: the name of the influx db to write to
    """

    def __init__(self, sql_engine: Engine = None, influx_client: InfluxDBClient = None,
                 influx_db_name: Text = INFLUX_DB_NAME):
        """
        :param sql_engine: the sql engine to save metadata and versions
        :param influx_client: the influx client to save the data to
        :param influx_db_name: the name of the influx db to write to
        """
        default_sql_engine, default_influx_client = create_default_dbs()
        # initializing sql
        self._sql_engine = sql_engine or default_sql_engine
        self._sql_session_maker = sessionmaker(bind=self._sql_engine)

        # initializing influx
        self._influx_db_name = influx_db_name
        self._influx_client = influx_client or default_influx_client
        self._influx_client.create_database(self._influx_db_name)

        self._uncreated_series = []
        self._entered = False

    def register_series(self, series: MonitoringSeries):
        """
        registers a new series

        :param series: the series to register
        """
        if self._entered:
            self._add_series_to_db(series)
        else:
            self._uncreated_series.append(series)

    def register_data(self, series: MonitoringSeries, data: Mapping,
                      version: Optional[Version] = None, op: Optional[_AbstractOp] = None):
        """
        registers a data point of a series

        :param series: the series corresponding to the data being registered
        :param data: the data point of this series to register
        :param version: if applicable the version corresponding to the op that generated the data being registered
        :param op: the op corresponding to the data being registered TODO implement behavior
        """
        processed_version = processed_op = None
        if version is not None:
            processed_version = self._process_version(version)
        if op is not None:
            processed_op = self._process_op(op)
        self._influx_client.write_points([{
            "measurement": series.series_name,
            "time": datetime.utcnow().isoformat(),
            "fields": data,
            "tags": {
                "version": processed_version,
                "op": processed_op
            }
        }])

    def _process_version(self, version: Version) -> Text:
        session = self._sql_session_maker()
        db_version = session.query(DBVersion).filter(*DBVersion.find_equivalent_filter_builder(
            version)).first()
        if not db_version:
            db_version = DBVersion.from_version(version)
            session.add(db_version)
        session.commit()
        return f"{db_version.major_checksum}.{db_version.minor_checksum}.{db_version.patch_checksum}"

    def _process_op(self, op: _AbstractOp) -> Text:
        return op.name

    def _add_series_to_db(self, series: MonitoringSeries):
        session = self._sql_session_maker()
        existing_series = session.query(_MonitoringSeriesMetadata).filter(
            _MonitoringSeriesMetadata.series_name == series.series_name).first()
        if existing_series:
            existing_series.graphical_display = series.graphical_display_format.value
            existing_series.numerical_display = series.numerical_display_format.value
            session.commit()
            return

        session.add(self._create_table_instance(series))
        session.commit()

    @staticmethod
    def _create_table_instance(series: MonitoringSeries):
        return _MonitoringSeriesMetadata(series_name=series.series_name,
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


