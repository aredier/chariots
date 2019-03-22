import uuid
from abc import ABC
from abc import abstractclassmethod
from typing import Type
from typing import List
from typing import Any
from typing import Tuple

import numpy as np


IntType = np.int32
FloatType = np.float32


class Requirement(ABC):
    """
    A marker represents a requirement for an op and qualifies the output of an op
    Markers' init should always have default value and be able to instantiate with epmpty
    arguments. If you want to change this behaviour, you will have to override the new_marker
    classmethod to account for your desired behavior
    """

    dype = None

    def __new__(cls, *args, **kwargs):
        raise ValueError("reauirements should not be instantiated")

    @classmethod
    def compatible(cls, other: Type["Requirement"]) -> bool:
        """
        checks if this Requirement is copatible with other, if not overiden the default 
        behavior checks that other is a subclass of cls
        
        Arguments:
            other {Type[Marker]} -- the Requirement to check against
        
        Returns:
            bool -- wether or not other can be considered as cls
        """

        return issubclass(other, cls)
    
    @classmethod
    def create_child(cls, name=None) -> Type["Marker"]:
        """
        creates a unique marker that will be accepted by this class but will only accept itself 
        """
        name = name or f"{cls.__name__}-sub"
        return type(name, (cls,), 
                    {"__doc__": f"automaticly generated marker generated from {cls.__name__}"})
    
    @classmethod
    def parse(cls, data: Any) -> Any:
        """
        parses raw data and casts it to the underlying type this Requirement represents
        if cls.dtype is set, this will default to instantiating it, otherwise, it will return the
        data unvhanged
        
        Arguments:
            data {Any} -- the data to parse
        
        Returns:
            Any -- the parsed data
        """

        if cls.dtype is None:
            return data
        return cls.dtype(data)

    @classmethod
    def combine(cls, left: Any, right: Any) -> Any:
        """
        how to combine two instances of the underlying type(s)
        
        Raises:
            NotImplementedError -- when the combine logic is not implemented
        """

        raise NotImplementedError(f"{cls.__name__} does not implement combine logic")
    
    @classmethod
    def as_marker(cls):
        """helper function that returns this type to provide a common interface with ops"""

        return cls



class Number(Requirement):
    """
    a marker that represents a single number (not Batched) underlying type is float32
    """

    dtype = FloatType


class Matrix(Requirement):
    """
    an marker for ops that output matrix-like data (np.arrays, sparse, ...)
    """
    
    shape = None
    parent_class = None 

    @classmethod
    def compatible(cls, other: Type["Requirement"]) -> bool:
        """will check that the type and shape are compatible"""
        if cls.parent_class is not None:
            if not cls.parent_class.compatible(other):
                return False
        elif not super().compatible(other):
            return False
        if cls.shape is None:
            return True
        if other.shape is None or len(other.shape) != len(cls.shape):
            return False
        for self_ind, other_ind in zip(cls.shape, other.shape):
            if self_ind is not None and self_ind != other_ind:
                return False
        return True
    
    @classmethod
    def with_shape_and_dtype(cls, shape: Tuple[int], dtype: Type) ->Type["Matrix"]:
        res = cls.create_child()
        res.shape = shape
        res.dtype = dtype
        res.parent_class = cls
        return res
        
    @classmethod
    def parse(cls, data: Any) -> Any:
        if cls.dtype is not None:
            return np.asarray(data, dtype=cls.dtype)
        try:
            return np.asarray(array_as_list, FloatType)
        except ValueError:
            return np.asarray(array_as_list)
        
    @classmethod
    def combine(cls, left: Any, right: Any) -> Any:
        """will combine the data along the first dimension"""
        return np.concatenate((np.asarray(left), np.asarray(right)))
        
