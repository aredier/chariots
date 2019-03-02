import uuid
from abc import ABC
from abc import abstractmethod
from typing import Type

class Marker(ABC):
    """
    A marker represents a requirement for an op and qualifies the output of an op
    Markers' init should always have default value and be able to instantiate with epmpty
    arguments. If you want to change this behaviour, you will have to override the new_marker
    classmethod to account for your desired behavior
    """

    @abstractmethod
    def compatible(self, other: "Marker") -> bool:
        pass 
    
    @classmethod
    def new_marker(cls) -> Type["Marker"]:
        """
        creates a unique marker that will be accepted by this class but will only accept itself 
        """
        class NewMarker(cls):
            identifier = uuid.uuid1()

            def compatible(self, other: "Marker") -> bool:
                return hasattr(other, "identifier") and other.identifier == self.identifier
        return NewMarker
    
    def as_marker(self) -> "Marker":
        return self

class Number(Marker):
    """
    a marker that represents a single number (not Batched)
    """
    def compatible(self, other: Marker) -> bool:
        return isinstance(other, Number)

class Matrix(Marker):
    """
    an marker for ops that output matrix-like data (np.arrays, sparse, ...)
    """
    def __init__(self, shape: tuple = None):
        self.shape = shape or (None,)
    
    def compatible(self, other: Marker) -> bool:
        return isinstance(other, Matrix) and self.shape == other.shape