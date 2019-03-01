from abc import ABC
from abc import abstractmethod
from abc import abstractproperty


class TrainableTrait(ABC):

    @abstractproperty
    def fited(self):
        """
        is the Training fited
        """

    @abstractmethod
    def fit(self, *args, **kwargs):
        """
        fit the trainable object
            :param *args: positional arguments to be overiden by children
            :param **kwargs: keywords argments to be overriden by childre 
        """

