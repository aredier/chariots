"""
Savers are used to persist and reload ops. A saver can be viewed as the basic abstraction of a file system
(interprets path) and always has a root path (that represents the path after which the saver will start
persisting data).

To use a specific saver, you need to pass it as a parameter of your `OpStoreServer` so that the op_store_client in terms
 knows how and where to persist your ops

.. testsetup::

    >>> import tempfile
    >>> import shutil

    >>> from chariots.op_store import OpStoreServer

    >>> app_path = tempfile.mkdtemp()
    >>> db_url = 'foo'

.. doctest::

    >>> my_saver = FileSaver(app_path)
    >>> op_store_client = OpStoreServer(my_saver, db_url)

.. testsetup::
    >>> shutil.rmtree(app_path)


For now chariots only provides a basic `FileSaver` and a `GoogleStorageSaver` but there are plans to add more in future
releases (in particular to support more cloud service providers such as aws s3).

savers are used to persist and retrieve information about ops, nodes and pipeline (such as versions, persisted
versions, datasets, and so on).

to create your own saver, you can subclass the :doc:`BaseSaver class<./chariots.base>`
"""
from ._base_saver import BaseSaver
from ._file_saver import FileSaver
from ._google_storage_saver import GoogleStorageSaver

__all__ = [
    'FileSaver',
    'GoogleStorageSaver',
    'BaseSaver'
]
