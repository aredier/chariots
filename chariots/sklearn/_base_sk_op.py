from typing import Any

import chariots.base
import chariots.versioning


class BaseSKOp(chariots.base._base_ml_op.BaseMLOp):
    """
    base Op class for all the supervised and unsupervised scikit-learn ops
    """

    # the class of the model to fit/predict
    model_class = None
    # the parameters to use to init the model
    model_parameters = chariots.versioning._versioned_field_dict.VersionedFieldDict(
        chariots.versioning._version_type.VersionType.MAJOR, {})

    def _init_model(self):
        """
        initialises the model
        :return the initialised model
        """
        return self.model_class(**self.model_parameters)

    def fit(self, *args, **kwargs):
        raise NotImplementedError("you need to define the fit behavior when subclassing `BaseSKOp`")

    def predict(self, *args, **kwargs) -> Any:
        raise NotImplementedError("you need to define the predict behavior when subclassing `BaseSKOp`")