"""
module that provides the main app for {{cookiecutter.project_name}}, to be
called using a chariots client
"""
import os
from pathlib import Path

from chariots import Chariots, op_store

from {{cookiecutter.project_name}}.pipelines.train_iris import train_iris
from {{cookiecutter.project_name}}.pipelines.pred_iris import pred_iris


LOCAL_FOLDER = os.path.join(str(Path(__file__).parents[1]),
                            '{{cookiecutter.project_name}}_local')


{% if cookiecutter.use_iris_example == 'y' -%}

op_store = op_store.OpStoreClient(None)

{{cookiecutter.project_name}}_app = Chariots(
    [train_iris, pred_iris],
    path=LOCAL_FOLDER,
    import_name='{{cookiecutter.project_name}}',
    op_store_client=op_store
)
{% else %}
# TODO add pipelines here
{{cookiecutter.project_name}}_app = Chariots(
    [],
    path=LOCAL_FOLDER,
    import_name='{{cookiecutter.project_name}}',
)
{%- endif %}
