from enum import Enum


class MLMode(Enum):
    """
    mode in which to put the op (prediction of training) enum
    """
    FIT = "fit"
    PREDICT = "predict"
    FIT_PREDICT = "fit_predict"
