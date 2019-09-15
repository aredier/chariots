========
chariots
========


.. image:: https://img.shields.io/pypi/v/chariots.svg
        :target: https://pypi.python.org/pypi/chariots

.. image:: https://img.shields.io/travis/aredier/chariots.svg
        :target: https://travis-ci.org/aredier/chariots

.. image:: https://readthedocs.org/projects/chariots/badge/?version=latest
        :target: https://chariots.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://img.shields.io/github/license/aredier/chariots?color=green
        :target: https://github.com/aredier/chariots/blob/master/LICENSE




chariots aims to be a complete framework to build and deploy versioned machine learning pipelines.

* Documentation: https://chariots.readthedocs.io.

Getting Started: 30 seconds to Chariots:
----------------------------------------
You can check the :doc:`documentation<https://chariots.readthedocs.io>` for a complete tutorial on getting started with
chariots, but here are the essentials:

you can create operations to execute steps in your pipeline:

.. doctest::

    >>> from chariots.sklearn import SKUnsupervisedOp, SKSupervisedOp
    >>> from chariots.versioning import VersionType, VersionedFieldDict, VersionedField
    >>> from sklearn.decomposition import PCA
    >>> from sklearn.linear_model import LogisticRegression
    ...
    ...
    >>> class PCAOp(SKUnsupervisedOp):
    ...     training_update_version = VersionType.MAJOR
    ...     model_parameters = VersionedFieldDict(VersionType.MAJOR, {"n_components": 2})
    ...     model_class = VersionedField(PCA, VersionType.MAJOR)
    ...
    >>> class LogisticOp(SKSupervisedOp):
    ...     training_update_version = VersionType.PATCH
    ...     model_class = LogisticRegression

Once your ops are created, you can create your various training and prediction pipelines:

.. testsetup::

    >>> from chariots._helpers.doc_utils import IrisFullDataSet

.. doctest::

    >>> from chariots import Pipeline, MLMode
    >>> from chariots.nodes import Node
    ...
    ...
    >>> train = Pipeline([
    ...     Node(IrisFullDataSet(), output_nodes=["x", "y"]),
    ...     Node(PCAOp(MLMode.FIT_PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
    ...     Node(LogisticOp(MLMode.FIT), input_nodes=["x_transformed", "y"])
    ... ], 'train')
    ...
    >>> pred = Pipeline([
    ...     Node(PCAOp(MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="x_transformed"),
    ...     Node(LogisticOp(MLMode.PREDICT), input_nodes=["x_transformed"], output_nodes=['__pipeline_output__'])
    ... ], 'pred')

Once all your pipelines have been created, deploying them is as easy as creating a creating a `Chariots` object:

.. testsetup::
    >>> import tempfile
    >>> import shutil

    >>> app_path = tempfile.mkdtemp()

.. doctest::

    >>> from chariots import Chariots
    ...
    ...
    >>> app = Chariots([train, pred], app_path, import_name='iris_app')


The `Chariots` class inherits from the `Flask` class so you can deploy this the same way you would any
:doc:`flask application<https://github.com/pallets/flask>`.

Once this the server is started, you can use the chariots client to query your machine learning micro-service from
python:

.. doctest::

    >>> from chariots import Client
    ...
    ...
    >>> client = Client()

.. testsetup::

    >>> from chariots import TestClient
    ...
    ...
    >>> client = TestClient(app)

with this client we will be

- training the models
- saving them and reloading the prediction pipeline (so that it uses the latest/trained version of our models)
- query some prediction

.. doctest::

    >>> client.call_pipeline(train)
    >>> client.save_pipeline(train)
    >>> client.load_pipeline(pred)
    >>> client.call_pipeline(pred, [[1, 2, 3, 4]])
    [1]

.. testsetup::

    >>> shutil.rmtree(app_path)

Features
--------

* versionable individual op
* easy pipeline building
* easy pipelines deployment
* ML utils (implementation of ops for most popular ML libraries with adequate `Versionedfield`) for sklearn and keras at first
* A CookieCutter template to properly structure your Chariots project

Comming Soon
------------

Some key features of Chariot are still in development and should be coming soon:

* Cloud integration (integration with cloud services to fetch and load models from)
* Graphql API to store and load information on different ops and pipelines (performance monitoring, ...)
* ABTesting

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.
`audreyr/cookiecutter-pypackage`_'s project is also the basis of the Chariiots project template

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypac
