"""Tests for `chariots` package."""
import json
import subprocess

import pytest

from click.testing import CliRunner

from chariots import cli


@pytest.fixture
def cookiecutter_config():
    """fixture of the cookie cutter config"""
    return {
        'project_name': 'iris',
        'author': 'Antoine Redier',
        'author_email': 'foo.bar@mail.com',
        'project_short_description': 'an awsome project :)',
        'use_iris_example': 'y',
        'use_cli': 'y',
        'use_git': 'y',
        'open_source_license': 'MIT license'
    }


@pytest.mark.skip(reason='Need to refacto the project template to make this pass')
def test_command_line_interface(cookiecutter_config):  # pylint: disable=redefined-outer-name
    """Test the CLI by generating the iris and running its tests"""
    runner = CliRunner()
    with runner.isolated_filesystem():
        with open('config.json', 'w') as config_file:
            json.dump(cookiecutter_config, config_file)
        result = runner.invoke(cli.main, ['new', '-c', 'config.json'])
        assert result.exit_code == 0
        try:
            subprocess.call(['pip', 'install', '-e', './'], cwd='iris/')
            assert subprocess.call(['py.test'], cwd='iris/') == 0
        except subprocess.CalledProcessError as err:
            print(err.output.decode('utf-8'))
            raise
