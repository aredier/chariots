How to parallelize your Chariots app using workers
==================================================

Once you have built your app you might want to deploy it using

.. testsetup::

    >>> import tempfile
    >>> import shutil
    >>> app_path = tempfile.mkdtemp()
    >>> my_pipelines = []

.. doctest::

    >>> from chariots import Chariots
    ...
    ...
    >>> app = Chariots(
    ...     my_pipelines,
    ...     path=app_path,
    ...     import_name="my_app"
    ... )


but you will soon discover that by default that all the pipeline's are executed in the main server process.
This is desirable by default as a lot of your pipeline executions (prediction, preprocessing, ...) are quick enough.
However you will probably have a couple pipelines that you need executed asynchronously (not blocking the server process)
and/or a different server/machine.

In order to achieve this, Chariots offers a worker api. You can either use the default RQ workers or subclass the
`BaseWorkerPool` class in order to create your own worker queue.

Using RQ Worker
---------------

RQ is a worker queue based off Redis. To integrate it with your Chariots app. You only need to link Redis to your app as
such:

.. testsetup::

    >>> import tempfile
    >>> import shutil
    >>> app_path = tempfile.mkdtemp()
    >>> my_pipelines = []

.. doctest::

    >>> from redis import Redis
    >>> from chariots import Chariots, workers
    ...
    ...
    >>> app = Chariots(
    ...     my_pipelines,
    ...     path=app_path,
    ...     import_name="my_app",
    ...     worker_pool=workers.RQWorkerPool(redis=Redis())
    ... )

you than have several options:

**using workers for all the pipelines in the app:**

.. doctest::

    >>> from redis import Redis
    >>> from chariots import Chariots, workers
    ...
    ...
    >>> app = Chariots(
    ...     my_pipelines,
    ...     path=app_path,
    ...     import_name="my_app",
    ...     worker_pool=workers.RQWorkerPool(redis=Redis()),
    ...     use_workers=True
    ... )


**using workers for all the calls to a specific pipeline**

.. testsetup::
    >>> from chariots import Pipeline
    >>> from chariots.nodes import Node
    >>> from chariots._helpers.doc_utils import AddOneOp, IsOddOp

.. doctest::

    >>> pipeline = Pipeline(use_worker=True, pipeline_nodes=[
    ...     Node(AddOneOp(), input_nodes=["__pipeline_input__"], output_nodes=["added_number"]),
    ...     Node(IsOddOp(), input_nodes=["added_number"], output_nodes=["__pipeline_output__"])
    ... ], name="async_pipeline")

**using workers for a specific call**

.. testsetup::

    >>> import time

    >>> from redis import Redis
    >>> from chariots import Pipeline, Chariots, TestClient
    >>> from chariots.workers import RQWorkerPool
    >>> from chariots._helpers.doc_utils import is_odd_pipeline
    >>> from chariots._helpers.test_helpers import RQWorkerContext
    >>> app = Chariots([is_odd_pipeline], app_path, import_name='simple_app', worker_pool=RQWorkerPool(Redis()))
    >>> client = TestClient(app)


.. doctest::

    >>> with RQWorkerContext():
    ...     response = client.call_pipeline(is_odd_pipeline, 4, use_worker=True)
    ...     print(response.job_status)
    ...     time.sleep(3)
    ...     response = client.fetch_job(response.job_id, is_odd_pipeline)
    ...     print(response.job_status)
    ...     print(response.value)
    JobStatus.queued
    JobStatus.done
    False

Creating your Own worker class
------------------------------

If RQ does not suit your needs, you can use another one. To integrate it with Cahriots you will need to subclass
the `BaseWorkerPool` class. you can find more information on BaseWorkerPool in the  :doc:`api docs <../api_docs/chariots.workers>`
