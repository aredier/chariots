"""
the sklearn module provides support for the scikit-learn framework.

this module provides two main classes (`SKSupervisedOp`, `SKUnsupervisedOp`) that need to be subclassed to be used. to
do so you will need to set the `model_class` class attribute and potentially the `model_parameters` class attribute.
this should be a :doc:`VersionedFieldDict<./chariots.versioning>` which defines the parameters your model should be
initialized with. As for other machine learning ops, you can override the `training_update_version` class attribute to
define which version will be changed when the operation is retrained:

.. testsetup::

    >>> from chariots import Pipeline, MLMode
    >>> from chariots.nodes import Node
    >>> from sklearn.decomposition import PCA
    >>> from chariots.sklearn import SKUnsupervisedOp
    >>> from chariots.versioning import VersionType, VersionedFieldDict, VersionedField
    >>> from chariots._helpers.doc_utils import IrisXDataSet

.. doctest::

    >>> class PCAOp(SKUnsupervisedOp):
    ...     training_update_version = VersionType.MAJOR
    ...     model_parameters = VersionedFieldDict(VersionType.MAJOR, {"n_components": 2,})
    ...     model_class = VersionedField(PCA, VersionType.MAJOR)

Once your op class is define, you can use it as any MLOp choosing your :doc:`MLMode<./chariots>` to define the behavior
of your operation (fit and/or predict):

.. doctest::

    >>> train_pca = Pipeline([Node(IrisXDataSet(), output_nodes=["x"]), Node(PCAOp(MLMode.FIT), input_nodes=["x"])],
    ...                      'train_pca')
"""
from ._base_sk_op import BaseSKOp
from ._sk_supervised_op import SKSupervisedOp
from ._sk_unsupervised_op import SKUnsupervisedOp

__all__ = [
    'SKSupervisedOp',
    'SKUnsupervisedOp',
    'BaseSKOp'
]
