import os

from chariots.monitoring import monitoring_interface


class FakeMonitoringTable(monitoring_interface.AbstractMonitoringTable):
    table_name = "fake"

    my_numerical_field = monitoring_interface.MonitoringField(
        dtype=monitoring_interface.FieldTypes.INT, default_value=None,
        optional=False
    )
    my_optional_numerical_field = monitoring_interface.MonitoringField(
        dtype=monitoring_interface.FieldTypes.INT, default_value=-1,
        optional=True
    )


def _test_csv_and_get_positions(file_line: str, *args):
    fields = file_line.strip().split(",")
    assert len(fields) == len(args)
    res = []
    for field in args:
        res.append(fields.index(field))
    return res

def test_monitoring_init(tmp_path):
    monitoring_dir = os.path.join(tmp_path, "monitoring")
    with monitoring_interface.CSVMonitoringInterface(path=monitoring_dir):
        pass
    assert os.path.isfile(os.path.join(monitoring_dir, "_all_fields.csv"))
    with open(os.path.join(monitoring_dir, "_all_fields.csv")) as main_monitoring_file:
        lines = main_monitoring_file.readlines()
        assert len(lines) == 1
        _test_csv_and_get_positions(lines[0], "_table_name", "_numerical_display_format",
                                    "_graphical_display_format")

def test_table_init(tmp_path):
    monitoring_dir = os.path.join(tmp_path, "monitoring")
    with monitoring_interface.CSVMonitoringInterface(path=monitoring_dir) as interface:
        _ = FakeMonitoringTable(interface)
    assert os.path.isfile(os.path.join(monitoring_dir, "_all_fields.csv"))
    with open(os.path.join(monitoring_dir, "_all_fields.csv")) as main_monitoring_file:
        lines = main_monitoring_file.readlines()
        assert len(lines) == 2
        positions = _test_csv_and_get_positions(lines[0], "_table_name",
                                                "_numerical_display_format",
                                                "_graphical_display_format")
        first_line_elmts = lines[1].strip().split(",")
        assert len(first_line_elmts) == 3
        assert first_line_elmts[positions[0]] == "fake"
        assert first_line_elmts[positions[1]] == str(
            monitoring_interface.SeriesNumericalDisplayFormat.TABLE
        )
        assert first_line_elmts[positions[2]] == str(
           monitoring_interface.SeriesGraphicalDisplayFormat.LINE_CHART
        )
    assert os.path.isfile(os.path.join(monitoring_dir, "fake.csv"))
    with open(os.path.join(monitoring_dir, "fake.csv")) as main_monitoring_file:
        lines = main_monitoring_file.readlines()
        assert len(lines) == 1
        _test_csv_and_get_positions(lines[0], "my_numerical_field", "my_optional_numerical_field")


def test_table_dump(tmp_path):
    monitoring_dir = os.path.join(tmp_path, "monitoring")
    with monitoring_interface.CSVMonitoringInterface(path=monitoring_dir) as interface:
        my_field = FakeMonitoringTable(interface)
        my_field.dump_data(my_numerical_field=4, my_optional_numerical_field=3)
        my_field.dump_data(my_numerical_field=5)
        my_field.dump_data.when.called_with(my_optional_numerical_field=3).should.throw(ValueError)
        my_field.flush()
    assert os.path.isfile(os.path.join(monitoring_dir, "fake.csv"))
    with open(os.path.join(monitoring_dir, "fake.csv")) as main_monitoring_file:
        lines = main_monitoring_file.readlines()
        assert len(lines) == 3
        positions = _test_csv_and_get_positions(lines[0], "my_numerical_field",
                                                "my_optional_numerical_field")

        first_line_elmts = lines[1].strip().split(",")
        assert len(first_line_elmts) == 2
        assert first_line_elmts[positions[0]] == "4"
        assert first_line_elmts[positions[1]] == "3"

        scd_line_elmts = lines[2].strip().split(",")
        assert len(first_line_elmts) == 2
        assert scd_line_elmts[positions[0]] == "5"
        assert scd_line_elmts[positions[1]] == "-1"

