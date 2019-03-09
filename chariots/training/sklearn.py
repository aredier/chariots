from sklearn.externals import joblib

from chariots.core.versioning import VersionField
from chariots.core.versioning import VersionType
from chariots.training.trainable_op import TrainableOp
from chariots.core.markers import Matrix
from chariots.core.saving import Savable


class SklearnOp(Savable, TrainableOp):

    model_cls = VersionField(VersionType.MINOR, default_factory=lambda: None)
    training_params = VersionField(VersionType.MINOR, default_factory=lambda: {})

    def __init__(self):
        self.model = self.model_cls(**self.training_params)

    def _inner_train(self, **kwargs):
        pass

    def _main(self, **kwargs) -> DataBatch:
        pass

    def _serialize(self, temp_file: IO):
        joblib.dump(self.model, temp_file)
    
    @classmethod
    def checksum(cls):
        return str(cls._build_version())
    
    @classmethod
    def identifiers(cls):
        return {"name": cls.name, "model_type": "sklearn"}


class OnlineSklearnSupervised(SklearnOp):

    # these have to be overiden when inheriting
    markers = [Matrix(None, 1)]
    requires = {"x": Matrix(())}
    training_requirements = {"y_train": Matrix(())}

    def __init__(self, **kwargs):
        self.training_requirements["x_train"] = self.requires["x"]
    
    @classmethod
    def factory(cls, x_marker, y_marker, model_cls, training_params = None, name = "some_sk_model", 
                description = ""):
        resulting_op = type(name, (cls), {"__doc__": description})
        resulting_op.requires = {"x": x_marker}
        resulting_op.raining_requirements = {"y_train": y_marker}
        resulting_op.model_cls = model_cls
        resulting_op.training_params = training_params or {}
        return resulting_op

    def _inner_train(self, x_train, y_train):
        self.model.partial_fit(x=x_train, y=y_train)
        
    def _main(self, x) -> DataBatch:
        return self.model.predict(x)


class OnlineSklearnTransformer(SklearnOp):
    pass


class SingleFitSkSupervised(SklearnOp):
    pass


class SingleFitSkTransformer(SklearnOp):
    pass