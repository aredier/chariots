from chariots.base import BaseOp


class XYSplit(BaseOp):

    def execute(self, df):

        return df.drop('target', axis=1), df.target
