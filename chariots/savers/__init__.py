"""
savers are used to persist and retrieve information about ops, nodes and pipeline (such as versions, persisted
versions, datasets, and so on).

A saver can be viewed as the basic abstraction of a file system (interprets path) and always has a root path (that
represents the path after which the saver will start persisting data).

For now chariots only provides a basic `FileSaver` saver but there are plans to add more in future releases (in
particular to support bottomless cloud storage solutions such as aws s3 and Google cloud storage).

to create your own saver, you can subclass the :doc:`BaseSaver class<./chariots.base>`

To use a specific saver in your app, you will need to specify the saver class and the root path of the saver in the
`Chariots` initialisation:

.. testsetup::

    >>> import tempfile
    >>> import shutil

    >>> from chariots import Chariots
    >>> from chariots.savers import FileSaver

    >>> app_path = tempfile.mkdtemp()
    >>> my_pipelines = []

.. doctest::

    >>> my_app = Chariots(app_pipelines=my_pipelines, path=app_path, saver_cls=FileSaver, import_name="my_app")

.. testsetup::
    >>> shutil.rmtree(app_path)

"""
from ._file_saver import FileSaver

__all__ = [
    'FileSaver'
]
