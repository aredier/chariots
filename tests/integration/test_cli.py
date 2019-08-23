#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `chariots` package."""
import json
import os
import subprocess

import pytest

from click.testing import CliRunner

from chariots import cli


@pytest.fixture
def cookiecutter_config():
    return {
        "project_name": "iris",
        "author": "Antoine Redier",
        "author_email": "foo.bar@mail.com",
        "project_short_description": "an awsome project :)",
        "use_iris_example": "y",
        "use_cli": "y",
        "use_git": "y",
        "open_source_license": "MIT license"
    }


def test_command_line_interface(cookiecutter_config):
    """Test the CLI."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        with open("config.json", "w") as config_file:
            json.dump(cookiecutter_config, config_file)
        result = runner.invoke(cli.main, ["new", "-c", "config.json"])
        assert result.exit_code == 0
        try:
            subprocess.call(["pip", "install","-e", "./"], cwd="iris/")
            assert subprocess.call(["py.test"], cwd="iris/") == 0
        except subprocess.CalledProcessError as err:
            print(err.output.decode("utf-8"))
            raise
