How to parallelize your Chariots app using workers
==================================================

Once you have built your app you might want to deploy it using

.. testsetup::

    >>> import tempfile
    >>> import shutil
    >>> from chariots.testing import TestOpStoreClient
    >>> app_path = tempfile.mkdtemp()
    >>> op_store_client = TestOpStoreClient(app_path)
    >>> my_pipelines = []

.. doctest::

    >>> from chariots.pipelines import PipelinesServer
    ...
    ...
    >>> app = PipelinesServer(
    ...     my_pipelines,
    ...     op_store_client=op_store_client,
    ...     import_name="my_app"
    ... )

.. testsetup::

    >>> shutil.rmtree(app_path)


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
    >>> from chariots.testing import TestOpStoreClient
    ...
    >>> app_path = tempfile.mkdtemp()
    >>> my_pipelines = []
    >>> op_store_client = TestOpStoreClient(app_path)
    >>> op_store_client.server.db.create_all()

.. doctest::

    >>> from redis import Redis
    >>> from chariots import workers, op_store
    >>> from chariots.op_store import savers
    >>> from chariots.pipelines import PipelinesServer
    ...
    ...
    >>> app = PipelinesServer(
    ...     my_pipelines,
    ...     op_store_client=op_store_client,
    ...     import_name="my_app",
    ...     worker_pool=workers.RQWorkerPool(redis=Redis())
    ... )

you than have several options:

    >>> from chariots.testing import TestOpStoreClient

**using workers for all the pipelines in the app:**

.. doctest::

    >>> from redis import Redis
    >>> from chariots import workers
    >>> from chariots.pipelines import PipelinesServer
    ...
    ...
    >>> app = PipelinesServer(
    ...     my_pipelines,
    ...     op_store_client=op_store_client,
    ...     import_name="my_app",
    ...     worker_pool=workers.RQWorkerPool(redis=Redis()),
    ...     use_workers=True
    ... )


**using workers for all the calls to a specific pipeline**

.. testsetup::

    >>> from chariots.pipelines import Pipeline
    >>> from chariots.pipelines.nodes import Node
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
    >>> from chariots.workers import RQWorkerPool
    >>> from chariots.testing import TestPipelinesClient
    >>> from chariots._helpers.doc_utils import is_odd_pipeline
    >>> from chariots._helpers.test_helpers import RQWorkerContext
    >>> app = PipelinesServer([is_odd_pipeline], op_store_client=op_store_client,
    ...                        import_name='simple_app', worker_pool=RQWorkerPool(Redis()))
    >>> client = TestPipelinesClient(app)


.. doctest::

    >>> with RQWorkerContext():
    ...     response = client.call_pipeline(is_odd_pipeline, 4, use_worker=True)
    ...     print(response.job_status)
    ...     time.sleep(5)
    ...     response = client.fetch_job(response.job_id, is_odd_pipeline)
    ...     print(response.job_status)
    ...     print(response.value)
    JobStatus.queued
    JobStatus.done
    False

.. testsetup::

    >>> shutil.rmtree(app_path)

Creating your Own worker class
------------------------------

If RQ does not suit your needs, you can use another one. To integrate it with Cahriots you will need to subclass
the `BaseWorkerPool` class. you can find more information on BaseWorkerPool in the  :doc:`api docs <../api_docs/chariots.workers>`


When Will a pipeline be executed in a worker?
_____________________________________________

As you can see in the Rq code examples, there are three ways to ask for pipelines to be executed in the worker pool:

* at the app level (for all calls to this app)
* at teh pipeline level (for all calls to this pipeline)
* at the request level (for this specific call)

Then if any of these are set to `True` for a call and the others are not specified (left unfilled). The call will
be executed in a worker. But if any of those is explicitly set to `False` the call will **not** be executed in a
pipeline (regardless of whether the others are set to true or not)
