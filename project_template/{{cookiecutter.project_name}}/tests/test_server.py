import multiprocessing as mp
import time

from click import testing
from chariots.backend.client import Client

{% if cookiecutter.use_cli == 'y' -%}
from {{cookiecutter.project_name}} import cli
{% else %}
from {{cookiecutter.project_name}}.app import {{cookiecutter.project_name}}_app
{%- endif %}
from {{cookiecutter.project_name}}.pipelines.download_iris import download_iris
from {{cookiecutter.project_name}}.pipelines.train_iris import train_iris
from {{cookiecutter.project_name}}.pipelines.pred_iris import pred_iris


def start_server():
    {% if cookiecutter.use_cli == 'y' -%}
    runner = testing.CliRunner()
    runner.invoke(cli.start)
    {% else %}
    {{cookiecutter.project_name}}_app.run()
    {%- endif %}

def test_server():
    process = mp.Process(target=start_server, args=())
    try:
        process.start()
        time.sleep(1)
        client = Client()

        {% if cookiecutter.use_cli == 'y' -%}
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

