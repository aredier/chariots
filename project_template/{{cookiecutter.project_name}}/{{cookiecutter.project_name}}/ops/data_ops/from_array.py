from chariots.base import BaseOp


class FromArray(BaseOp):
    """op that converts a numpy array into a JSON serializable list"""

    def execute(self, np_array):
        return np_array.tolist()
