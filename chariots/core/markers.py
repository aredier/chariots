from abc import ABC
from abc import abstractmethod

class Marker(ABC):
    """
    A marker represents a requirement for an op and qualifies the output of an op
    """

    @abstractmethod
    def compatible(self, other: "Marker") -> bool:
        pass 

class Number(Marker):
    def compatible(self, other: Marker) -> bool:
        return isinstance(other, Number)

class Matrix(Marker):
    """
    an marker for ops that output matrix-like data (np.arrays, sparse, ...)
    """
    def __init__(self, shape: tuple):
        self.shape = shape
    
    def compatible(self, other: Marker) -> bool:
        return isinstance(other, Matrix) and self.shape == other.shape