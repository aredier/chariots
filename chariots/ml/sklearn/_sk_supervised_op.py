"""module to support sci-kit learn supervised models"""
from typing import Any

from ._base_sk_op import BaseSKOp
from ... import versioning


class SKSupervisedOp(BaseSKOp):
    """
    Op base class to create supervised models using the scikit learn framework.,
    If using the `MLMode.FIT` or `MLMode.FIT_PREDICT`, you will need to link this op to a X and a y upstream node:

    .. testsetup::

        >>> from chariots.pipelines import Pipeline
        >>> from chariots.pipelines.nodes import Node
        >>> from chariots.ml import MLMode
        >>> from chariots._helpers.doc_utils import PCAOp, LogisticOp, IrisFullDataSet

    .. doctest::

        >>> train_logistics = Pipeline([
        ...     Node(IrisFullDataSet(), output_nodes=["x", "y"]),
        ...     Node(PCAOp(MLMode.PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
        ...     Node(LogisticOp(MLMode.FIT), input_nodes=["x_transformed", "y"])
        ... ], 'train_logistics')

    and if you are using the op with the `MLMode.PREDICT` mode you will only need to link the op to an X upstream node:

    .. doctest::

        >>> pred = Pipeline([
        ...     Node(IrisFullDataSet(),input_nodes=['__pipeline_input__'], output_nodes=["x"]),
        ...     Node(PCAOp(MLMode.PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
        ...     Node(LogisticOp(MLMode.PREDICT), input_nodes=["x_transformed"], output_nodes=['__pipeline_output__'])
        ... ], 'pred')


    To change the behavior of the Op, you can:

    * change the `predict_function` class attribute with a new `VersionedField` (to use `predict_proba` for instance)
    * change the `fit_extra_parameters` class attribute with a new `VersionedFieldDict` (to pass some new parameters
      during prediction)
    """

    predict_function = versioning.VersionedField('predict', versioning.VersionType.MAJOR)
    fit_extra_parameters = versioning.VersionedFieldDict(versioning.VersionType.MAJOR, {})

    def fit(self, X, y):  # pylint: disable=arguments-differ
        """
        method used by the operation to fit the underlying model

        DO NOT TRY TO OVERRIDE THIS METHOD.

        :param X: the input that the underlying supervised model will fit on (type must be compatible with the sklearn
                  lib such as numpy arrays or pandas data frames)
        :param y: the output that hte underlying supervised model will fit on (type must be compatible with the sklearn
                  lib such as numpy arrays or pandas data frames)
        """
        self._model.fit(X, y, **self.fit_extra_parameters)

    def predict(self, X) -> Any:  # pylint: disable=arguments-differ
        """
        method used internally by the op to predict with the underlying model.

        DO NOT TRY TO OVERRIDE THIS METHOD.

        :param X: the input the model has to predict on. (type must be compatible with the sklearn lib such as numpy
                  arrays or pandas data frames)

        """

        return getattr(self._model, self.predict_function)(X)
