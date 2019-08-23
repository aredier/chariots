{%- if cookiecutter.use_iris_example == "y" %}
import multiprocessing as mp
import time

{% endif -%}
from click import testing
{%- if cookiecutter.use_iris_example == "y" %}
from chariots.backend.client import Client
{% endif -%}

{% if cookiecutter.use_cli == "y" -%}
from {{cookiecutter.project_name}} import cli
{% else %}
from {{cookiecutter.project_name}}.app import {{cookiecutter.project_name}}_app
{%- endif -%}
{%- if cookiecutter.use_iris_example == "y" -%}
{%- if cookiecutter.use_cli == 'n' -%}
from {{cookiecutter.project_name}}.pipelines.download_iris import download_iris
from {{cookiecutter.project_name}}.pipelines.train_iris import train_iris
{%- endif -%}
from {{cookiecutter.project_name}}.pipelines.pred_iris import pred_iris
{%- endif %}

def start_server():
    """starts the server using the cli"""
    {% if cookiecutter.use_cli == 'y' -%}
    runner = testing.CliRunner()
    runner.invoke(cli.start)
    {% else %}
    {{cookiecutter.project_name}}_app.run()
    {%- endif %}

def test_server():
    """tests that the pipelines are running correctly"""
    {% if cookiecutter.use_iris_example== "y" -%}
    process = mp.Process(target=start_server, args=())
    try:
        process.start()
        time.sleep(1)
        client = Client()

        {%- if cookiecutter.use_cli == 'y' %}
        runner = testing.CliRunner()
        runner.invoke(cli.download_and_train)
        {% else %}
        client.call_pipeline(download_iris)
        client.call_pipeline(train_iris)
        client.save_pipeline(train_iris)
        client.load_pipeline(pred_iris)
        {%- endif %}
        assert client.call_pipeline(
            pred_iris,
            pipeline_input=[[1, 2, 3, 4]]
        ) == [1]
    finally:
        process.kill()
        process.join()
    {% else %}
    # TODO write a test
    pass
    {%- endif %}
