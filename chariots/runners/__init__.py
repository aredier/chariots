"""
runners are used to execute Pipelines: they define in what order and how each node of the pipeline should be executed.

For the moment Chariots only provides a basic sequential runner that executes each operation of a pipeline one after the
other in a single threat however we have plans to introduce new runners (process and thread based ones as well as some
cluster computing one) in future releases.

You can use runners directly if you want to execute your pipeline manually:

.. testsetup::
    >>> from chariots.runners import SequentialRunner
    >>> from chariots._helpers.doc_utils import is_odd_pipeline

.. doctest::

    >>> runner = SequentialRunner()
    >>> runner.run(is_odd_pipeline, 5)
    True

or you can set the default runner of your app and it will be used every time a pipeline execution is called:

.. testsetup::

    >>> import tempfile
    >>> import shutil
    >>> from chariots import Chariots
    >>> from chariots.op_store._op_store_client import TestOpStoreClient

    >>> app_path = tempfile.mkdtemp()
    >>> op_store_client = TestOpStoreClient(app_path)
    >>> op_store_client.server.db.create_all()

.. doctest::

    >>> my_app = Chariots(app_pipelines=[is_odd_pipeline], runner=SequentialRunner(), op_store_client=op_store_client,
    ...                   import_name="my_app")

.. testsetup::

    >>> shutil.rmtree(app_path)
"""
from ._sequential_runner import SequentialRunner


__all__ = [
    'SequentialRunner'
]
