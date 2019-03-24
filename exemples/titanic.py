import os

from sklearn.ensemble import RandomForestClassifier
import numpy as np
from chariots.io import csv
from chariots.core import ops
from chariots.core import requirements
from chariots.training.sklearn import SingleFitSkSupervised
from chariots.training import evaluation


TRAIN_PATH = os.environ.get("TITANIC_TRAIN_PATH")

numerical_features = requirements.Matrix.create_child("numerical_features").with_shape_and_dtype((None, 2), np.float32)
# preprocessed_names = requirements.Matrix.create_child("preprocessed_names").with_shape_and_dtype((None, 1), np.str)
# preprocessed_sex = requirements.Matrix.create_child("preprocessed_sex").with_shape_and_dtype((None, 1), np.str)
# preprocessed_embarked = requirements.Matrix.create_child("preprocessed_embarked").with_shape_and_dtype((None, 1), np.str)
y = requirements.Matrix.create_child("survived-pred").with_shape_and_dtype((None, 1), np.int32)
y_evaluate = requirements.Matrix.create_child("survived-true").with_shape_and_dtype((None, 1), np.int32)


model_cls = SingleFitSkSupervised.factory(
    numerical_features,
    y,
    model_cls = RandomForestClassifier,
    name="our_glorious_model"
)

model = model_cls()
model.attach_evaluation(evaluation.ClassificationMetrics(y_evaluate, y))

with csv.CSVTap(TRAIN_PATH, {numerical_features: ["Age", "Fare"], y: ["Survived"]}, skip_nan=True) as tap:
    model.fit(tap)

with csv.CSVTap(TRAIN_PATH, {numerical_features: ["Age", "Fare"], y_evaluate: ["Survived"]}, skip_nan=True) as tap:
    print(model.evaluate(tap))