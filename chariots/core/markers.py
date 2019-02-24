from abc import ABC
from abc import abstractmethod

class Marker(ABC):

    @abstractmethod
    def compatible(self, other: "Marker") -> bool:
        pass 

class Number(Marker):
    def compatible(self, other: Marker) -> bool:
        return isinstance(other, Number)

class Matrix(Marker):
    def __init__(self, shape: tuple):
        self.shape = shape
    
    def compatible(self, other: Marker) -> bool:
        return isinstance(other, Matrix) and self.shape == other.shape