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

numerical_features = requirements.Matrix.create_child("numerical_features").with_shape_and_dtype((None, 2), np.float32)
not_processed_names = requirements.Matrix.create_child("preprocessed_names").with_shape_and_dtype((None, 1), np.str)
not_processed_sex = requirements.Matrix.create_child("preprocessed_sex").with_shape_and_dtype((None, 1), np.str)
not_processed_class = requirements.Matrix.create_child("not_processed_class").with_shape_and_dtype((None, 1), np.str)
none_preprocessed_embarked = requirements.Matrix.create_child("preprocessed_embarked").with_shape_and_dtype((None, 1), np.str)
y = requirements.Matrix.create_child("survived-pred").with_shape_and_dtype((None, 1), np.int32)
y_evaluate = requirements.Matrix.create_child("survived-true").with_shape_and_dtype((None, 1), np.int32)

is_master = numerical_features.create_child("is_master")

class IsMasterOp(ops.BaseOp):
    is_reversed = versioning.VersionField(subversion=versioning.SubVersionType.MAJOR,
                                          target_version=versioning.VersionType.RUNTIME,
                                          default_value=False)

    def _main(self, names: not_processed_names) -> is_master:
        if self.is_reversed:
            return np.array([[int(not bool(re.match(".*Master", name[0])))] for name in names])
        return np.array([[int(bool(re.match(".*Master", name[0])))] for name in names])

is_master_op = IsMasterOp()

sex_encoder_cls = SingleFitSkTransformer.factory(
    not_processed_sex,
    numerical_features.create_child("sex_preprocessed"),
    model_cls = OneHotEncoder,
    name="sex_encoder",
    training_params = {"sparse": False}
)

class_encoder_cls = SingleFitSkTransformer.factory(
    not_processed_class,
    numerical_features.create_child("class_preprocessed"),
    model_cls = OneHotEncoder,
    name="class_encoder",
    training_params = {"sparse": False}
)

model_cls = SingleFitSkSupervised.factory(
    numerical_features,
    y,
    model_cls = RandomForestClassifier,
    name="our_glorious_model"
)

def train(train_path):
    class_encoder = class_encoder_cls()
    sex_encoder = sex_encoder_cls()
    model = model_cls()

    with csv.CSVTap(train_path, {numerical_features: ["Age", "Fare"], not_processed_sex: ["Sex"], not_processed_names: ["Name"], not_processed_class: ["Pclass"], y: ["Survived"]}, skip_nan=True) as tap:
        sex_training, class_training, features = ops.Split(3)(tap)

        class_encoder.fit(class_training)
        sex_encoder.fit(sex_training)

        sex_encoder = sex_encoder(features)
        class_encoder = class_encoder(sex_encoder)
        is_master_op = IsMasterOp()(class_encoder)
        model.fit(is_master_op)
    return class_encoder, sex_encoder, model

def test(test_path, class_encoder, sex_encoder, model):
    model.attach_evaluation(evaluation.ClassificationMetrics(y_evaluate, y))
    with csv.CSVTap(test_path, {numerical_features: ["Age", "Fare"],not_processed_sex: ["Sex"],
                                not_processed_names: ["Name"], not_processed_class: ["Pclass"],
                                y_evaluate: ["Survived"]}, skip_nan=True) as tap:
        features = sex_encoder(tap)
        features = class_encoder(features)

        features = IsMasterOp()(features)
        print(model.evaluate(features))

if __name__ == "__main__":
    saver = FileSaver("/tmp/chariots/")
    should_save = False
    try:
        model = model_cls.load(saver)
        class_encoder = class_encoder_cls.load(saver)
        sex_encoder = sex_encoder_cls.load(saver)
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
