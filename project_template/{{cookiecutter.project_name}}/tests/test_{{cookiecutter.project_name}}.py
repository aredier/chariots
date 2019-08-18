import pytest
from chariots.backend.client import TestClient


from {{cookiecutter.project_name}}.app import {{cookiecutter.project_name}}_app
from {{cookiecutter.project_name}}.pipelines.download_iris import download_iris
from {{cookiecutter.project_name}}.pipelines.train_iris import train_iris
from {{cookiecutter.project_name}}.pipelines.pred_iris import pred_iris

@pytest.fixture
def {{cookiecutter.project_name}}_test_client():
    return TestClient({{cookiecutter.project_name}}_app)

def test_iris_app({{cookiecutter.project_name}}_test_client):
    {{cookiecutter.project_name}}_test_client.call_pipeline(download_iris)
    {{cookiecutter.project_name}}_test_client.call_pipeline(train_iris)
    {{cookiecutter.project_name}}_test_client.save_pipeline(train_iris)
    {{cookiecutter.project_name}}_test_client.load_pipeline(pred_iris)
    assert {{cookiecutter.project_name}}_test_client.call_pipeline(pred_iris, pipeline_input=[[1, 2, 3, 4]]) == [1]
