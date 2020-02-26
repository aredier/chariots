Iris Tutorial
=============

In this beginners tutorial we will build a small `Chariots` server to serve predictions on the famous iris dataset.
If you want to see the final result, you can produce it directly using the `chariots new` command (see the
:doc:`chariots template <../template>` for more info).

before starting, we will create a new project by calling the `new` command in the parent directory of where we want our
project to be and leave all the default options

.. code-block:: console

    chariots new

Ops
---

we first need to design the individual ops we will build our pipelines from.

Data Ops
^^^^^^^^

First we will need an op that downloads the dataset, so in `iris/ops/data_ops/download_iris.py`

.. doctest::

    >>> import pandas as pd
    >>> from chariots.base import BaseOp
    >>> from sklearn import datasets
    ...
    ...
    >>> class DownloadIris(BaseOp):
    ...
    ...     def execute(self):
    ...         iris = datasets.load_iris()
    ...         df = pd.DataFrame(data=iris['data'], columns=iris['feature_names'])
    ...         df["target"] = iris["target"]
    ...         return df

Machine Learning Ops
^^^^^^^^^^^^^^^^^^^^
we will than need to build our various machine learning ops. For this example we will be using a PCA and than a
Random Forest in our pipeline. We will place those ops in the `iris.ops.model_ops` subpackage

.. doctest::

    >>> from sklearn.decomposition import PCA
    >>> from chariots.versioning import VersionType, VersionedFieldDict
    >>> from chariots.sklearn import SKUnsupervisedOp
    ...
    ...
    >>> class IrisPCA(SKUnsupervisedOp):
    ...
    ...     model_class = PCA
    ...     model_parameters = VersionedFieldDict(
    ...         VersionType.MAJOR,
    ...         {
    ...             "n_components": 2,
    ...         }
    ...     )

.. doctest::

    >>> from chariots.versioning import VersionType, VersionedFieldDict
    >>> from chariots.sklearn import SKSupervisedOp
    >>> from sklearn.ensemble import RandomForestClassifier
    ...
    ...
    >>> class IrisRF(SKSupervisedOp):
    ...
    ...     model_class = RandomForestClassifier
    ...     model_parameters = VersionedFieldDict(VersionType.MINOR, {"n_estimators": 5, "max_depth": 2})


Preprocessing Ops
^^^^^^^^^^^^^^^^^

we will not be using preprocessing ops per say but we will need an op that splits our saved dataset between `X` and `y`
as otherwise we will not be able to separate the two.

.. doctest::

    >>> from chariots.base import BaseOp
    ...
    ...
    >>> class XYSplit(BaseOp):
    ...
    ...     def execute(self, df):
    ...         return df.drop('target', axis=1), df.target


Pipelines
---------

We will than need to build our pipelines using the nodes we have just created:

Data Pipelines
^^^^^^^^^^^^^^

We have our op that downloads the dataset. We than need to feed this dataset into a data saving node that will persist
it for future uses (as the iris dataset is quite light, we could wire the download directly into the training pipeline
but we will persist it to demonstrate that dynamic).

.. doctest::

    >>> from chariots import Pipeline
    >>> from chariots.nodes import DataSavingNode, Node
    >>> from chariots.serializers import CSVSerializer
    ...
    ...
    >>> download_iris = Pipeline(
    ...     [
    ...         Node(DownloadIris(), output_nodes="iris_df"),
    ...         DataSavingNode(serializer=CSVSerializer(), path="iris.csv",
    ...                        input_nodes=["iris_df"])
    ...     ], "download_iris"
    ... )

Machine Learning Pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^

Once we have our data set saved, we will need to use it to train our models, we will than create a training pipeline:


.. testsetup::

    >>> from chariots._helpers.test_helpers import FromArray

.. doctest::

    >>> from chariots import MLMode, Pipeline
    >>> from chariots.nodes import DataLoadingNode, Node
    >>> from chariots.serializers import CSVSerializer
    ...
    ...
    >>> train_iris = Pipeline(
    ...     [
    ...         DataLoadingNode(serializer=CSVSerializer(), path="iris.csv",
    ...                         output_nodes="iris"),
    ...         Node(XYSplit(), input_nodes=["iris"], output_nodes=["raw_X", "y"]),
    ...         Node(IrisPCA(MLMode.FIT_PREDICT), input_nodes=["raw_X"],
    ...              output_nodes="pca_X"),
    ...         Node(IrisRF(MLMode.FIT), input_nodes=["pca_X", "y"])
    ...     ], "train_iris"
    ... )

Once the models will be trained, we will need to provide a pipeline for serving our models to our users. To do so, we
will create a pipeline that takes some user provided values (raws of the iris format) and retruns a prediction to the
user:

.. doctest::
    >>> from chariots import MLMode, Pipeline
    >>> from chariots.nodes import Node
    >>> pred_iris = Pipeline(
    ...
    ...
    ...     [
    ...         Node(IrisPCA(MLMode.PREDICT), input_nodes=["__pipeline_input__"],
    ...              output_nodes="x_pca"),
    ...         Node(IrisRF(MLMode.PREDICT), input_nodes=["x_pca"],
    ...              output_nodes="pred"),
    ...     Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
    ...     ], "pred_iris"
    ... )


App & Client
------------

Once our pipelines are all done, we will only need to create `Chariots` server to be able to serve our pipeline:

.. testsetup::

    >>> import tempfile
    >>> import shutil
    >>> app_path = tempfile.mkdtemp()

.. doctest::

    >>> from chariots import Chariots
    ...
    ...
    >>> app = Chariots(
    ...     [download_iris, train_iris, pred_iris],
    ...     path=app_path,
    ...     import_name="iris_app"
    ... )

Once this is done we only need to start our server as we would with any other `Flask`app (the `Chariots` type inherits
from the `Flask` class). For instance using the cli in the folder containing our `app.py`:

.. code-block:: console

    flask

our server is now running and we can execute our pipelines using the chariots client:

.. doctest::

    >>> from chariots import Client
    ...
    ...
    >>> client = Client()
    ...

.. testsetup::

    >>> from chariots import TestClient
    >>> client = TestClient(app)

we will need to execute several steps before getting to a prediction:

- download the dataset
- train the operations
- save the trained machine learning ops
- reload the prediction pipeline (to use the latest/trained version of the machine learning ops)

.. doctest::

    >>> res = client.call_pipeline(download_iris)
    >>> res = client.call_pipeline(train_iris)
    >>> client.save_pipeline(train_iris)
    >>> client.load_pipeline(pred_iris)
    ...
    >>> res = client.call_pipeline(pred_iris, [[1, 2, 3, 4]])
    >>> res.value
    [1]


.. testsetup::

    >>> shutil.rmtree(app_path)
