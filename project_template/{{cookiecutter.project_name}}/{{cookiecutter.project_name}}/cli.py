"""
module providing some cli helpers provided by {{cookiecutter.project_name}}
"""
import click
from chariots.backend.client import Client

from {{cookiecutter.project_name}}.app import {{cookiecutter.project_name}}_app
from {{cookiecutter.project_name}}.pipelines.download_iris import download_iris
from {{cookiecutter.project_name}}.pipelines.train_iris import train_iris
from {{cookiecutter.project_name}}.pipelines.pred_iris import pred_iris


@click.group()
def {{cookiecutter.project_name}}_cli():
    pass


@{{cookiecutter.project_name}}_cli.command()
def start():
    """
    start the {{cookiecutter.project_name}} server to be used by remote
     and local clients
    """

    {{cookiecutter.project_name}}_app.run()


{% if cookiecutter.use_iris_example == 'y' -%}
@{{cookiecutter.project_name}}_cli.command()
def download_and_train():
    """
    example of how to use the CLI to execute some interesting pipelines
    once the chariots server is started
    """

    client = Client()

    client.call_pipeline(download_iris)
    client.call_pipeline(train_iris)
    client.save_pipeline(train_iris)
    client.load_pipeline(pred_iris)
{%- endif %}
