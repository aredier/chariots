
from chariots._core import ops
import pandas as pd
from sklearn import datasets


class DownloadIris(ops.AbstractOp):
    """
    op that downloads the iris dataset from sci-kit and outputs it as pandas
    dataframe
    """
    
    def execute(self):
        iris = datasets.load_iris()
        df = pd.DataFrame(data=iris['data'], columns=iris['feature_names'])
        df["target"] = iris["target"]
        return df
