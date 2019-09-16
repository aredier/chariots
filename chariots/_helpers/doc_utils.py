"""
module with some example nodes, operations ... to produce beautifull doctests :)
"""
from collections import Counter

from keras.utils import to_categorical
from sklearn import datasets
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
import pandas as pd
from sklearn.model_selection import train_test_split

from chariots import Pipeline, MLMode
from chariots.base import BaseOp
from chariots.nodes import Node, DataSavingNode, DataLoadingNode
from chariots.serializers import CSVSerializer, DillSerializer
from chariots.sklearn import SKSupervisedOp, SKUnsupervisedOp
from chariots.versioning import VersionedFieldDict, VersionType, VersionedField


class AddOneOp(BaseOp):

    def execute(self, op_input):
        return op_input + 1


class IsOddOp(BaseOp):

    def execute(self, op_input):
        return bool(op_input % 2)


is_odd_pipeline = Pipeline([
    Node(IsOddOp(), input_nodes=["__pipeline_input__"], output_nodes=["__pipeline_output__"])
], "simple_pipeline")


class IrisFullDataSet(BaseOp):

    def execute(self, *args, **kwargs):
        iris = datasets.load_iris()

        df = pd.DataFrame(data=iris['data'],
                          columns=iris['feature_names'])
        return (df.loc[:, ['sepal length (cm)', 'sepal width (cm)', 'petal length (cm)', 'petal width (cm)']],
                iris["target"])


class IrisDF(BaseOp):

    def execute(self, *args, **kwargs):
        iris = datasets.load_iris()

        df = pd.DataFrame(data=iris['data'],
                          columns=iris['feature_names'])
        df["target"] = iris["target"]
        return df


class TrainTestSplit(BaseOp):

    def execute(self, ds_df):
        return tuple(train_test_split(ds_df, test_size=0.25, random_state=42))


class AnalyseDataSetOp(BaseOp):

    def execute(self, df):
        return Counter(df.target)


save_train_test = Pipeline([
    Node(IrisDF(), output_nodes='df'),
    Node(TrainTestSplit(), input_nodes=['df'], output_nodes=['train_df', 'test_df']),
    DataSavingNode(serializer=CSVSerializer(), path='/train.csv', input_nodes=['train_df']),
    DataSavingNode(serializer=DillSerializer(), path='/test.pkl', input_nodes=['test_df'])

], "save")

load_and_analyse_iris = Pipeline([
    DataLoadingNode(serializer=CSVSerializer(), path='/train.csv', output_nodes=["train_df"]),
    Node(AnalyseDataSetOp(), input_nodes=["train_df"], output_nodes=["__pipeline_output__"]),
], "analyse")


class IrisXDataSet(BaseOp):

    def execute(self, *args, **kwargs):
        iris = datasets.load_iris()

        df = pd.DataFrame(data=iris['data'],
                          columns=iris['feature_names'])
        return df.loc[:, ['sepal length (cm)', 'sepal width (cm)', 'petal length (cm)', 'petal width (cm)']]


class PCAOp(SKUnsupervisedOp):
    training_update_version = VersionType.MAJOR
    model_parameters = VersionedFieldDict(VersionType.MAJOR, {"n_components": 2})
    model_class = VersionedField(PCA, VersionType.MAJOR)


class LogisticOp(SKSupervisedOp):
    training_update_version = VersionType.PATCH
    model_class = LogisticRegression


train_pca = Pipeline([Node(IrisXDataSet(), output_nodes=["x"]), Node(PCAOp(MLMode.FIT), input_nodes=["x"])],
                     'train_pca')

train_logistics = Pipeline([
    Node(IrisFullDataSet(), output_nodes=["x", "y"]),
    Node(PCAOp(MLMode.PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
    Node(LogisticOp(MLMode.FIT), input_nodes=["x_transformed", "y"])
], 'train_logistics')

pred = Pipeline([
    Node(IrisFullDataSet(), input_nodes=['__pipeline_input__'], output_nodes=["x"]),
    Node(PCAOp(MLMode.PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
    Node(LogisticOp(MLMode.PREDICT), input_nodes=["x_transformed"], output_nodes=['__pipeline_output__'])
], 'pred')


class Categorize(BaseOp):

    def execute(self, dataset):
        return to_categorical(dataset)
