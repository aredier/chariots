# -*- coding: utf-8 -*-

"""Console script for chariots."""
import json
import os
import sys
import click
from pathlib import Path

from cookiecutter.main import cookiecutter

import chariots


@click.group()
def main():
    """Console scripts for chariots."""
    pass


@main.command()
@click.option('-c', '--config-file', 'config_file', type=click.Path(exists=True))
def new(config_file=None):
    """
    creates a new chariot project.
    this will open an interactive cookiecutter session with parameters to customize your projects
    """
    template_path = os.path.join(str(Path(chariots.__file__).parents[1]), "project_template")
    if config_file is None:
        return cookiecutter(template_path)
    with open(config_file) as config_file:
        config = json.load(config_file)
    return cookiecutter(template_path, extra_context=config, no_input=True)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
