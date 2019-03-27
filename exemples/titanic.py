import os
import re

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder

from chariots.core import ops, requirements, versioning
from chariots.core.pipeline import Pipeline
from chariots.core.saving import FileSaver
from chariots.io import csv
from chariots.training import evaluation
from chariots.training.sklearn import (SingleFitSkSupervised,
                                       SingleFitSkTransformer)

TRAIN_PATH = os.environ.get("TITANIC_TRAIN_PATH")
TEST_PATH = os.environ.get("TITANIC_TEST_PATH")

NumericalFeatures = requirements.Matrix.create_child("numerical_features").with_shape_and_dtype((None, 2), np.float32)
NotProcessedNames = requirements.Matrix.create_child("preprocessed_names").with_shape_and_dtype((None, 1), np.str)
NotProcessedSex = requirements.Matrix.create_child("preprocessed_sex").with_shape_and_dtype((None, 1), np.str)
NotProcessedClass = requirements.Matrix.create_child("not_processed_class").with_shape_and_dtype((None, 1), np.str)
NotPreprocessedEmbarked = requirements.Matrix.create_child("preprocessed_embarked").with_shape_and_dtype((None, 1), np.str)
y = requirements.Matrix.create_child("survived-pred").with_shape_and_dtype((None, 1), np.int32)
YEvaluate = requirements.Matrix.create_child("survived-true").with_shape_and_dtype((None, 1), np.int32)

is_master = NumericalFeatures.create_child("is_master")

class IsMasterOp(ops.BaseOp):
    is_reversed = versioning.VersionField(subversion=versioning.SubVersionType.MAJOR,
                                          target_version=versioning.VersionType.RUNTIME,
                                          default_value=False)

    def _main(self, names: NotProcessedNames) -> is_master:
        if self.is_reversed:
            return np.array([[int(not bool(re.match(".*Master", name[0])))] for name in names])
        return np.array([[int(bool(re.match(".*Master", name[0])))] for name in names])

is_master_op = IsMasterOp()

SexEncoderOp = SingleFitSkTransformer.factory(
    NotProcessedSex,
    NumericalFeatures.create_child("sex_preprocessed"),
    model_cls = OneHotEncoder,
    name="sex_encoder",
    training_params = {"sparse": False}
)

ClassEncoderOp = SingleFitSkTransformer.factory(
    NotProcessedClass,
    NumericalFeatures.create_child("class_preprocessed"),
    model_cls = OneHotEncoder,
    name="class_encoder",
    training_params = {"sparse": False}
)

ModelOp = SingleFitSkSupervised.factory(
    NumericalFeatures,
    y,
    model_cls = RandomForestClassifier,
    name="our_glorious_model"
)

def train(train_path):
    class_encoder = ClassEncoderOp()
    sex_encoder = SexEncoderOp()
    model = ModelOp()

    with csv.CSVTap(train_path, {NumericalFeatures: ["Age", "Fare"], NotProcessedSex: ["Sex"], NotProcessedNames: ["Name"], NotProcessedClass: ["Pclass"], y: ["Survived"]}, skip_nan=True) as tap:
        sex_training, class_training, features = ops.Split(3)(tap)

        class_encoder.fit(class_training)
        sex_encoder.fit(sex_training)

        sex_encoder = sex_encoder(features)
        class_encoder = class_encoder(sex_encoder)
        is_master_op = IsMasterOp()(class_encoder)
        model.fit(is_master_op)
    return class_encoder, sex_encoder, model

def test(test_path, class_encoder, sex_encoder, model):
    model.attach_evaluation(evaluation.ClassificationMetrics(YEvaluate, y))
    with csv.CSVTap(test_path, {NumericalFeatures: ["Age", "Fare"],NotProcessedSex: ["Sex"],
                                NotProcessedNames: ["Name"], NotProcessedClass: ["Pclass"],
                                YEvaluate: ["Survived"]}, skip_nan=True) as tap:
        features = sex_encoder(tap)
        features = class_encoder(features)

        features = IsMasterOp()(features)
        print(model.evaluate(features))

if __name__ == "__main__":
    saver = FileSaver("/tmp/chariots/")
    should_save = False
    try:
        model = ModelOp.load(saver)
        class_encoder = ClassEncoderOp.load(saver)
        sex_encoder = SexEncoderOp.load(saver)
    except FileNotFoundError:
        should_save = True
        print("\nno models found, training\n")
        class_encoder, sex_encoder, model = train(TRAIN_PATH)

    try:
        test(TEST_PATH, class_encoder, sex_encoder, model)
    except ValueError as e:
        should_save = True
        print("\ntraining version not working anymore, retraining\n")
        test(TEST_PATH, *train(TRAIN_PATH))
    if should_save:
        print("\nsaving\n")
        model.save(saver=saver)
        class_encoder.save(saver=saver)
        sex_encoder.save(saver=saver)
