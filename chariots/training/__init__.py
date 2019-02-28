from abc import ABC
from abc import abstractmethod
from abc import abstractproperty


class TrainableTrait(ABC):

    @abstractproperty
    def fited(self):
        pass

    @abstractmethod
    def fit(self, *args, **kwargs):
        pass

