import os


import pytest
from influxdb import InfluxDBClient
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

from chariots.monitoring import monitoring_interface

pytestmark = pytest.mark.skipif(os.environ.get("CHARIOTS_SKIP_MONITORING_TESTS", False), "env not right to perform monitoring tests")

@pytest.fixture
def connectors(tmp_path):
    print(tmp_path)
    db_connection = create_engine(f'sqlite:////{tmp_path}/chariots.db', convert_unicode=True, echo=False)
    influx_client = InfluxDBClient('localhost', 8086, 'root', 'root', "chariots_test")
    influx_client.query("DROP DATABASE chariots_test")
    return db_connection, influx_client


class FakeMonitoringTable(monitoring_interface.MonitoringSeries):
    series_name = "fake"

    my_numerical_field = monitoring_interface.MonitoringField(
        dtype=monitoring_interface.FieldTypes.INT, default_value=None,
        optional=False
    )
    my_optional_numerical_field = monitoring_interface.MonitoringField(
        dtype=monitoring_interface.FieldTypes.INT, default_value=-1,
        optional=True
    )


def test_monitoring_init(connectors):
    with monitoring_interface.MonitoringInterface(*connectors, influx_db_name="chariots_test"):
        pass
    iengine = inspect(connectors[0])
    assert set(iengine.get_table_names()) == {monitoring_interface._MonitoringSeriesMetadata.__tablename__,
                                              monitoring_interface.DBVersion.__tablename__}


def test_table_init(connectors):
    with monitoring_interface.MonitoringInterface(*connectors, influx_db_name="chariots_test") as interface:
        _ = FakeMonitoringTable(interface)

    sql_engine = connectors[0]
    session = sessionmaker(bind=sql_engine)()
    res = session.query(monitoring_interface._MonitoringSeriesMetadata).all()
    assert len(res) == 1
    assert res[0].series_name == "fake"
    assert res[0].numerical_display == monitoring_interface.SeriesNumericalDisplayFormat.TABLE.value
    assert res[0].graphical_display == monitoring_interface.SeriesGraphicalDisplayFormat.LINE_CHART.value




def test_table_dump(connectors):
    with monitoring_interface.MonitoringInterface(*connectors, influx_db_name="chariots_test") as interface:
        my_field = FakeMonitoringTable(interface)
        my_field.dump_data(my_numerical_field=4, my_optional_numerical_field=3)
        my_field.dump_data(my_numerical_field=5)
        my_field.dump_data.when.called_with(my_optional_numerical_field=3).should.throw(ValueError)

    influx_data = list(connectors[1].query("select * from fake;")[("fake", None)])
    assert len(influx_data) == 2
    print(influx_data[0])
    assert influx_data[0].get("my_numerical_field", False)
    assert influx_data[0]["my_numerical_field"] == 4
    assert influx_data[0]["my_optional_numerical_field"] == 3
    assert influx_data[1]["my_numerical_field"] == 5
    assert influx_data[1]["my_optional_numerical_field"] == -1

