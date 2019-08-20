"""
module that provides the main app for {{cookiecutter.project_name}}, to be
called using a chariots client
"""
import os
from pathlib import Path

from chariots.backend.app import Chariot

from {{cookiecutter.project_name}}.pipelines.download_iris import download_iris
from {{cookiecutter.project_name}}.pipelines.train_iris import train_iris
from {{cookiecutter.project_name}}.pipelines.pred_iris import pred_iris


LOCAL_FOLDER = os.path.join(str(Path(__file__).parents[1]),
                            "{{cookiecutter.project_name}}_local")


{% if cookiecutter.use_iris_example == 'y' -%}
{{cookiecutter.project_name}}_app = Chariot(
    [download_iris, train_iris, pred_iris],
    path=LOCAL_FOLDER,
    import_name="{{cookiecutter.project_name}}"
)
{% else %}
# TODO add pipelines here
{{cookiecutter.project_name}}_app = Chariot(
    [],
    path=LOCAL_FOLDER,
    import_name="{{cookiecutter.project_name}}",
)
{%- endif %}
