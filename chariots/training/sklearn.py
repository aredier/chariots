import os
from typing import IO
from typing import Text
from typing import Optional
from typing import Type, Mapping

import numpy as np
from sklearn.externals import joblib
from sklearn.base import BaseEstimator

from chariots.core.requirements import Matrix
from chariots.core.requirements import Requirement
from chariots.core.requirements import FloatType
from chariots.core.saving import Savable
from chariots.core.versioning import VersionField
from chariots.core.versioning import SubVersionType
from chariots.helpers.types import DataBatch
from chariots.training.trainable_op import TrainableOp


class SklearnOp(TrainableOp):
    """abstract op that handles sklearn models (saving, factory, ...)
    """


    model_cls: Type[BaseEstimator] = VersionField(SubVersionType.MINOR, default_factory=lambda: None)
    training_params: Mapping = VersionField(SubVersionType.MINOR, default_factory=lambda: {})

    # these have to be overiden when inheriting
    markers = [Matrix.with_shape_and_dtype((None, 1), FloatType)]
    requires = {"x": Matrix}
    training_requirements = {}
    _na_resilient = False

    def __init__(self):
        self.model = self.model_cls(**self.training_params)  # pylint: disable=not-callable, not-a-mapping
        self.training_requirements["x_train"] = self.requires["x"]

    def _inner_train(self, **kwargs):
        pass

    def _main(self, **kwargs) -> DataBatch:
        pass

    def _serialize(self, temp_dir: Text):
        super()._serialize(temp_dir)
        with open(os.path.join(temp_dir, "model.pkl"), "wb") as file:
            joblib.dump(self.model, file)

    @classmethod
    def _deserialize(cls, temp_dir: Text) -> "Savable":
        res = super()._deserialize(temp_dir)
        with open(os.path.join(temp_dir, "model.pkl"), "rb") as file:
            res.model = joblib.load(file)
        res._is_fited = True
        return res

    @classmethod
    def identifiers(cls):
        return {"name": cls.name, "model_type": "sklearn"}

    @classmethod
    def factory(cls, x_marker: Requirement, y_marker: Requirement, model_cls: BaseEstimator,
                training_params: Optional[dict] = None, name: Text = "some_sk_model",
                description: Text = "") -> Type:
        """creates a new trainable op class inheriting from `cls` this allows to quicly produce
            ops without having to define a full class by hand

        Arguments:
            x_marker {Marker} -- the input marker of the required input data
            y_markerMarker {[type]} -- the marker of the op (as output data) in cases of supervised
                this will also be added as the training requirement for y_train
            model_cls {BaseEstimator} -- the class of the model to instanciate

        Keyword Arguments:
            training_params {Optional[dict]} -- the training params of the model (default: {None})
            name {Text} -- the name of the op (default: {"some_sk_model"})
            description {Text} --  a short description of the op that will be added 
            to its docstring (default: {""})

        Returns:
            Type -- the resulting class
        """
        resulting_op = type(name, (cls,), {"__doc__": description})
        resulting_op.requires = {"x": x_marker}
        resulting_op.model_cls = model_cls
        resulting_op.training_params = training_params or {}
        resulting_op.markers = [y_marker]
        return resulting_op


class OnlineSklearnSupervised(SklearnOp):

    # these have to be overiden when inheriting
    training_requirements = {"y_train": Matrix.with_shape_and_dtype((None, 1), FloatType)}

    def _inner_train(self, x_train, y_train):
        na_idxs = np.isnan(x_train).any(axis=1)
        x_train = x_train[~na_idxs]
        y_train = y_train[~na_idxs]
        self.model.partial_fit(X=x_train, y=y_train)

    def _main(self, x) -> DataBatch:
        return self.model.predict(x)

    @classmethod
    def factory(cls, x_marker, y_marker, model_cls, training_params = None, name = "some_sk_model", 
                description = ""):

        resulting_op = super().factory(x_marker, y_marker, model_cls, training_params, name,
                                       description)
        resulting_op.training_requirements = {"y_train": y_marker}
        return resulting_op


class OnlineSklearnTransformer(SklearnOp):

    def _inner_train(self, x_train):
        na_idxs = np.isnan(x_train).any(axis=1)
        x_train = x_train[~na_idxs]
        self.model.partial_fit(x=x_train)

    def _main(self, x) -> DataBatch:
        return self.model.transform(x)



class SingleFitSkSupervised(SklearnOp):
    # these have to be overiden when inheriting
    training_requirements = {"y_train": Matrix.with_shape_and_dtype((None, 1), FloatType)}

    # TODO add an error when refiting
    def _inner_train(self, x_train, y_train):
        na_idxs = np.isnan(x_train).any(axis=1)
        x_train = x_train[~na_idxs]
        y_train = y_train[~na_idxs]
        self.model.fit(X=x_train, y=y_train)

    def _main(self, x) -> DataBatch:
        return self.model.predict(x)

    @classmethod
    def factory(cls, x_marker, y_marker, model_cls, training_params = None, name = "some_sk_model", 
                description = ""):

        resulting_op = super().factory(x_marker, y_marker, model_cls, training_params, name,
                                       description)
        resulting_op.training_requirements = {"y_train": y_marker}
        return resulting_op


class SingleFitSkTransformer(SklearnOp):

    def _inner_train(self, x_train):
        na_idxs = np.isnan(x_train).any(axis=1)
        x_train = x_train[~na_idxs]
        self.model.fit(x_train)

    def _main(self, x) -> DataBatch:
        return self.model.transform(x)