from typing import IO
from typing import Text
from typing import Optional
from typing import Type

from sklearn.externals import joblib
from sklearn.base import BaseEstimator

from chariots.core.markers import Matrix
from chariots.core.markers import Marker
from chariots.core.saving import Savable
from chariots.core.versioning import VersionField
from chariots.core.versioning import VersionType
from chariots.helpers.types import DataBatch
from chariots.training.trainable_op import TrainableOp


class SklearnOp(Savable, TrainableOp):
    """abstract op that handles sklearn models (saving, factory, ...)
    """


    model_cls = VersionField(VersionType.MINOR, default_factory=lambda: None)
    training_params = VersionField(VersionType.MINOR, default_factory=lambda: {})

    # these have to be overiden when inheriting
    markers = [Matrix((None, 1))]
    requires = {"x": Matrix(())}
    training_requirements = {}

    def __init__(self):
        self.model = self.model_cls(**self.training_params)
        self.training_requirements["x_train"] = self.requires["x"]

    def _inner_train(self, **kwargs):
        pass

    def _main(self, **kwargs) -> DataBatch:
        pass

    def _serialize(self, temp_file: IO):
        joblib.dump(self.model, temp_file)

    def _deserialize(cls, file: IO) -> "Savable":
        res = cls()
        res.model = joblib.load(file)
    
    @classmethod
    def checksum(cls):
        return str(cls._build_version())
    
    @classmethod
    def identifiers(cls):
        return {"name": cls.name, "model_type": "sklearn"}

    @classmethod
    def factory(cls, x_marker: Marker, y_marker: Marker, model_cls: BaseEstimator,
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
    training_requirements = {"y_train": Matrix((None, 1))}

    def _inner_train(self, x_train, y_train):
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
        self.model.partial_fit(x=x_train)
        
    def _main(self, x) -> DataBatch:
        return self.model.transform(x)



class SingleFitSkSupervised(SklearnOp):
    # these have to be overiden when inheriting
    training_requirements = {"y_train": Matrix((None, 1))}

    # TODO add an error when refiting
    def _inner_train(self, x_train, y_train):
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
        print(self.model.__class__.__name__)
        self.model.fit(x_train)
        
    def _main(self, x) -> DataBatch:
        return self.model.transform(x)