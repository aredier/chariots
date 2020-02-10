#!/usr/bin/env python

import os

PROJECT_DIRECTORY = os.path.realpath(os.path.curdir)

def remove_file(filepath):
    os.remove(os.path.join(PROJECT_DIRECTORY, filepath))

if __name__ == '__main__':

    if '{{cookiecutter.use_iris_example}}' == 'n':
        remove_file('{{cookiecutter.project_name}}/pipelines/download_iris.py')
        remove_file('{{cookiecutter.project_name}}/pipelines/train_iris.py')
        remove_file('{{cookiecutter.project_name}}/pipelines/pred_iris.py')
        remove_file('{{cookiecutter.project_name}}/ops/data_ops/download_iris.py')
        remove_file('{{cookiecutter.project_name}}/ops/feature_ops/x_y_split.py')
        remove_file('{{cookiecutter.project_name}}/ops/model_ops/iris_pca.py')
        remove_file('{{cookiecutter.project_name}}/ops/model_ops/iris_rf.py')
        remove_file('tests/test_{{cookiecutter.project_name}}.py')
        remove_file('notebooks/example_notebook.ipynb')

    if '{{cookiecutter.use_cli}}' == 'n':
        remove_file('{{cookiecutter.project_name}}/cli.py')

    if '{{cookiecutter.use_git}}' == 'n':
        remove_file('.gitignore')

    if 'Not open source' == '{{ cookiecutter.open_source_license }}':
        remove_file('LICENSE')
