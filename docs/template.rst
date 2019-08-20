Chariots template
=================

chariots provides a template to create Chariot templates that take care of the boilerplate involved.
This template is inspired by the DataScience `audreyr/cookiecutter-pypackage`_  and `drivendata/cookiecutter-data-science`_ project templates so you may find some similarities

to create a new project, just use

.. code-block:: console

    $ chariots new

you can then follow the prompts to customize your template (if you don't know what to put, follow the defaults).
If you want a minimalist example (using the classic iris dataset), you can put `y`in the `use_iris_example` parameter.

File Structure
--------------

the file structure of the project is as follows:

.. code-block:: console

    .
    ├── AUTHORS.rst
    ├── LICENSE
    ├── MANIFEST.in
    ├── Makefile
    ├── README.rst
    ├── docs
    │   ├── Makefile
    │   ├── authors.rst
    │   ├── conf.py
    │   ├── index.rst
    │   ├── installation.rst
    │   ├── make.bat
    │   └── modules.rst
    ├── iris
    │   ├── __init__.py
    │   ├── app.py
    │   ├── cli.py
    │   ├── ops
    │   │   ├── __init__.py
    │   │   ├── data_ops
    │   │   │   └── __init__.py
    │   │   ├── feature_ops
    │   │   │   └── __init__.py
    │   │   └── model_ops
    │   │       └── __init__.py
    │   └── pipelines
    │       └── __init__.py
    ├── iris_local
    │   ├── data
    │   └── ops
    ├── notebooks
    │   └── example_notebook.ipynb
    ├── requirements.txt
    ├── requirements_dev.txt
    ├── setup.cfg
    ├── setup.py
    └── tests
        └── test_server.py`


the `iris` folder (it will take the name of your project) is the main module of the project. It contains three main parts:

- the ops module contains all your Chariot ops. this is where most of the models/preprocessing goes (in their specific subfolders)
- the pipelines module defines the different pipelines of your project
- the app module provides the Chariots app that you can use to deploy your pipeline

the iris_local folder is where the chariots app will be mounted on (to load and save data/models) by default

the notebooks folder is where you can put you exploration and reporting notebooks

tools
-----

the template provides several tools in order to facilitate development:

a cli interface that include

.. code-block:: console

    $ my_great_project start

to start the server

a makefile to build the doc, clean the project and more

and more to come...
-


.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypac
.. _`drivendata/cookiecutter-data-science`: https://github.com/drivendata/cookiecutter-data-science