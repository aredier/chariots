# pylint: disable=invalid-name
"""
module with some example nodes, operations ... to produce beautifull doctests :)
"""
from collections import Counter

from keras.utils import to_categorical
from sklearn import datasets
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
import pandas as pd

from chariots import Pipeline, MLMode
from chariots.base import BaseOp
from chariots.nodes import Node
from chariots.serializers import CSVSerializer, DillSerializer
from chariots.sklearn import SKSupervisedOp, SKUnsupervisedOp
from chariots.versioning import VersionedFieldDict, VersionType, VersionedField


class AddOneOp(BaseOp):
    """op that adds 1 o its input"""

    def execute(self, op_input):  # pylint: disable=arguments-differ
        return op_input + 1


class IsOddOp(BaseOp):
    """op that returns whether or not its input is odd"""

    def execute(self, op_input):  # pylint: disable=arguments-differ
        return bool(op_input % 2)


is_odd_pipeline = Pipeline([
    Node(IsOddOp(), input_nodes=['__pipeline_input__'], output_nodes=['__pipeline_output__'])
], 'simple_pipeline')


class IrisFullDataSet(BaseOp):
    """op that loads and returns the Iris dataset with X, y as two outputs"""

    def execute(self, *args, **kwargs):
        iris = datasets.load_iris()

        df = pd.DataFrame(data=iris['data'],
                          columns=iris['feature_names'])
        return (df.loc[:, ['sepal length (cm)', 'sepal width (cm)', 'petal length (cm)', 'petal width (cm)']],
                iris['target'])


class IrisDF(BaseOp):
    """iris dataset op with the target as a column of the df"""

    def execute(self, *args, **kwargs):
        iris = datasets.load_iris()

        df = pd.DataFrame(data=iris['data'],
                          columns=iris['feature_names'])
        df['target'] = iris['target']
        return df


class TrainTestSplit(BaseOp):
    """train test splits the input data frame"""

    def execute(self, ds_df):  # pylint: disable=arguments-differ
        return tuple(train_test_split(ds_df, test_size=0.25, random_state=42))


class AnalyseDataSetOp(BaseOp):
    """op that gives the Counter of the df's target"""

    def execute(self, df):  # pylint: disable=arguments-differ
        return Counter(df.target)


class IrisXDataSet(BaseOp):
    """op that only returns the X part of the Iris DataSet"""

    def execute(self, *args, **kwargs):
        iris = datasets.load_iris()

        df = pd.DataFrame(data=iris['data'],
                          columns=iris['feature_names'])
        return df.loc[:, ['sepal length (cm)', 'sepal width (cm)', 'petal length (cm)', 'petal width (cm)']]


class PCAOp(SKUnsupervisedOp):
    """an op that performs two component PCA"""
    training_update_version = VersionType.MAJOR
    model_parameters = VersionedFieldDict(VersionType.MAJOR, {'n_components': 2})
    model_class = VersionedField(PCA, VersionType.MAJOR)


class LogisticOp(SKSupervisedOp):
    """an op for a logistic regression"""
    training_update_version = VersionType.PATCH
    model_class = LogisticRegression


train_pca = Pipeline([Node(IrisXDataSet(), output_nodes=['x']), Node(PCAOp(MLMode.FIT), input_nodes=['x'])],
                     'train_pca')

train_logistics = Pipeline([
    Node(IrisFullDataSet(), output_nodes=['x', 'y']),
    Node(PCAOp(MLMode.PREDICT), input_nodes=['x'], output_nodes='x_transformed'),
    Node(LogisticOp(MLMode.FIT), input_nodes=['x_transformed', 'y'])
], 'train_logistics')

pred = Pipeline([
    Node(IrisFullDataSet(), input_nodes=['__pipeline_input__'], output_nodes=['x']),
    Node(PCAOp(MLMode.PREDICT), input_nodes=['x'], output_nodes='x_transformed'),
    Node(LogisticOp(MLMode.PREDICT), input_nodes=['x_transformed'], output_nodes=['__pipeline_output__'])
], 'pred')


class Categorize(BaseOp):
    """returns the input dataset as categorical"""

    def execute(self, dataset):  # pylint: disable=arguments-differ
        return to_categorical(dataset)
