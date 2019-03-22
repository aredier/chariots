# pylint: disable=no-member
"""
base op of chariots
"""
import random
import functools
import inspect
from abc import ABC
from abc import abstractmethod
from abc import abstractclassmethod
from abc import ABCMeta
from typing import Optional
from typing import List
from typing import Mapping
from typing import Type
from typing import Text
from typing import Any

from chariots.core.dataset import DataSet, ORIGIN
from chariots.core.requirements import Requirement
from chariots.core.versioning import _extract_versioned_fields
from chariots.core.versioning import VersionField
from chariots.core.versioning import _VersionField
from chariots.core.versioning import Version
from chariots.core.versioning import SubVersionType
from chariots.core.versioning import VERSIONING_PRE
from chariots.helpers.types import DataBatch
from chariots.helpers.types import Requirements
from chariots.helpers.utils import SplitPuller
from chariots.helpers.utils import SplitPusher


class AbstractOp(ABC):
    """
    base op of a pipeline
    the main entry point of the op is going to be the perform method.
    there are several fields that are needed to create an op:
        - marker : corresponds to the markers of this op, these will be searched by the next
          op in the pipeline as parameters for their _main method
    """
    saving_version: Version = None
    name: Text = None
    previous_op = None
    # TODO these should be part of the major version of the op
    markers: List[Requirement] = None
    requires: Requirements = None

    # wether or not  this op should carry on the upstream version changes silently or not
    # this is `True` by default as basic ops have no accionable way to fix a deprecation
    # this is different from a trainable op where the op can retrain to cope with the change (most
    # of the time)
    _carry_on_verision = True

    def __new__(cls, *args, **kwargs):
        """
        checks that fields are implemented
        """
        cls.name = cls.name or cls.__name__
        cls.saving_version, cls.runtime_version = cls._build_version()
        cls.markers = cls.markers or []
        
        cls.requires = cls.requires or {}
        cls.requires = {key: value.as_marker() for key, value in cls.requires.items()}
        instance = super().__new__(cls)
        return instance

    # TODO use class property for those two
    # https://stackoverflow.com/questions/5189699/how-to-make-a-class-property
    @classmethod
    def _build_version(cls) -> Version:
        return _extract_versioned_fields(cls)
    
    def __call__(self, other: "AbstractOp") -> "AbstractOp":
        """
        used to determine the ancestor of an op
        """
        if not isinstance(other, AbstractOp):
            raise ValueError("call does only work with single ops. if you want another"\
                             "behavior, override the __Call__ method") 
        self._check_compatibility(other, self.requires)
        if self._carry_on_verision:
            self._link_versions(other)
        self.previous_op = other
        return self

    @property
    def compounded_markers_and_version_str(self):
        if self.previous_op is None:
            return [(m, str(self.runtime_version)) for m in self.markers]
        return [*[(m, str(self.runtime_version)) for m in self.markers], 
                *[marker for marker in self.previous_op.compounded_markers_and_version_str
                  if not any(requirement.compatible(marker[0]) for requirement
                            in self.requires.values())]]

    def _link_versions(self, other: "AbstractOp"):
        self.runtime_version.major.link(other.runtime_version.major)
        self.runtime_version.minor.link(other.runtime_version.minor)
        self.runtime_version.patch.link(other.runtime_version.patch)

    
    def __getattribute__(self, attribute: Text) -> Any:
        """gets the attribute and unwrapts it if it is a `VersionField` instance
        
        Arguments:
            attribute {Text} -- the attribute name
        
        Returns:
            Any -- the attribute's value
        """
        underlying = object.__getattribute__(self, attribute)
        if isinstance(underlying, VersionField):
            field = object.__getattribute__(self, VERSIONING_PRE + attribute)
            return field.value
        return underlying
    
    def __setattr__(self, attribute: Text, value: Any):
        """sets an attribute or the attriubute's inner value if the attribute is an instance of 
        class `VersionField`
        
        Arguments:
            attribute {Text} -- the attribute name
            value {Any} -- the desired value
        """

        try:
            underlying = object.__getattribute__(self, attribute)
            if isinstance(underlying, VersionField):
                field = object.__getattribute__(self, VERSIONING_PRE + attribute)
                field.set(value)
            else:
                object.__setattr__(self, attribute, value)
        except AttributeError:
            object.__setattr__(self, attribute, value)
    
    @staticmethod
    def _check_compatibility(other: "AbstractOp", requirements: Requirements):
        missing = next((required for required in requirements.items()
                        if all(not required[1].compatible(marker[0])
                               for marker in other.compounded_markers_and_version_str)),
                       None)
        if missing is not None:
            raise ValueError(f"requirement {missing} not fulfiled by {other.name}")

    @abstractmethod
    def perform(self) -> "DataSet":
        """
        the main entry point of an op that should perform the op's ancestors and th op itself and 
        returns the resulting DataSet
        """
        pass
    
    @property
    def ready(self):
        """
        is the op ready to be performed
        """
        if self.previous_op:
            return self.previous_op.ready
        return True
    
    @classmethod
    @functools.lru_cache()
    def as_marker(cls) -> Requirement:
        """
        produces a marker that corresponds to this op
        """
        if len(cls.markers) != 1:
            raise ValueError("using more or less than one marker for an op is ambigous to"\
                             " produce marker")
        return cls.markers[0].create_child()
    

class BaseOp(AbstractOp):
    """
    BaseOp is a simple implementation of an op were _main is performed on each data batch 
    individually in order to do that, a litle magic (not too much I hope) is added to determine
    wich part of the data batch should become which argument of the _main method:
    The key of each requirement is used as the parameter_name of the corresponding argument in _main
    hence all the arguments of _main must be keys of the required dict
    """
    def __new__(cls, *args, **kwargs):
        cls._interpret_signature()
        instance = super().__new__(cls)
        return instance 

    @abstractmethod
    def _main(self, **kwargs) -> DataBatch:
        """
        function to be overriden to create an op, the kwargs dict will have the same keys as the 
        requirements and the values will be the part of the batch corresponding to the marker.
        """

    def perform(self) -> "DataSet":
        """
        implementation of perform for the base op
        """
        if self.previous_op is None:
            raise ValueError("this pipeline doesn't seem to have a tap, can't get the data flowing")
        return self._map_op(self.previous_op.perform())
    
    def _map_op(self, data_set: DataSet):
        """
        maps itself to a dataset
        """
        return DataSet.from_op(map(self._perform_single, data_set))
    
    def _perform_single(self, data: DataBatch):
        """
        performs the argument resolution executes the op on a databatch
        """
        args_dict, unused_data = self._resolve_arguments(data, self.requires)
        op_res = self._main(**args_dict)
        wraped_res = dict(zip(self.markers, op_res if isinstance(op_res, tuple) else (op_res,)))
        wraped_res.update(unused_data)
        return wraped_res
    
    def _resolve_arguments(self, data: dict, requirements: Requirements):
        res = {}
        for arg_name, marker in requirements.items():
            for data_marker in data:
                if marker.compatible(data_marker):
                    res[arg_name] = data.pop(data_marker) 
                    break
        return res, data
    
    @staticmethod
    def _find_valid_requirements(signature: inspect.Signature) -> Mapping[Text, Type[Requirement]]:
        requirements = {}

        # markers and requires has not been updated manually so we have to try to interpret
        for arg_name, arg in signature.parameters.items():
            if arg_name == "self":
                continue
            if arg.annotation is inspect.Signature.empty:
                raise ValueError("requirements were not set mannually and type annotation is empty,"\
                                " cannot infer previous ops requirements")
            if not issubclass(arg.annotation, Requirement):
                raise ValueError("requirements were not set mannually and type annotation is  not"\
                                "subclass of Requirements, cannot infer previous op requirement")
            requirements[arg_name] = arg.annotation
        return requirements
    
    @staticmethod
    def _find_marker(signature: inspect.Signature) -> Type[Requirement]:
        if signature.return_annotation is inspect.Signature.empty:
            raise ValueError("the markers of this op are not set manually and no return type "\
                                "is set on _main, cannot infer markers")
        if not issubclass(signature.return_annotation, Requirement):
            raise ValueError("the markers of this op are not set manually and the return type"\
                                "set on _main is not a Requirement, cannot infer markers")
        return signature.return_annotation


    @classmethod
    def _interpret_signature(cls):
        main_sig = inspect.signature(cls._main)
        if cls.requires is None:
            cls.requires = cls._find_valid_requirements(main_sig)
        
        if cls.markers is None:
            cls.markers = [cls._find_marker(main_sig)]

    
class Split(AbstractOp):
    """
    split operation that creates several downstreams from a single upstream
    be carefull, splits are not free as they have to do a deepcopy of each batch to
    prevent data races
    """

    name = "split"

    def __init__(self, n_splits: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._n_splits = n_splits
    
    @property
    def markers(self):
        return self.previous_op.markers
    
    def __call__(self, other: "AbstractOp") -> List["_SplitRes"]:
        self.previous_op = other
        self._pusher = SplitPusher(self._n_splits)
        self._link_versions(other)
        return [_SplitRes(puller, self) for puller in self._pusher.pullers]
    
    def perform(self):
        if self.previous_op is None:
            raise ValueError("this pipeline doesn't seem to have a tap, can't get"\
                             " the data flowing")
        self._pusher.set_iterator(self.previous_op.perform())

    @property
    def compounded_markers_and_version_str(self):
        if self.previous_op is None:
            return []
        return self.previous_op.compounded_markers_and_version_str

    
class _SplitRes(AbstractOp):
    """
    downstream op of a split (returned by Split.__call__)
    """

    name = "split_puller"

    def __init__(self, puller: SplitPuller, split_op: Split, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._puller = puller
        self.previous_op = split_op
        self._link_versions(split_op)

    @property
    def markers(self):
        return self.previous_op.markers
    
    def __call__(self, other: "AbstractOp") -> "AbstractOp":
        raise ValueError("split puller should not be called directly")
    
    def perform(self) -> DataSet:
        self.previous_op.perform()
        return DataSet.from_op(self._puller)

    @property
    def compounded_markers_and_version_str(self):
        if self.previous_op is None:
            return []
        return self.previous_op.compounded_markers_and_version_str

class Merge(AbstractOp):
    """
    Op that merges sevreral pipelines in a single one
    """

    name = "merge"

    def __init__(self, *args, **kwargs):
        self.previous_op = None
        super().__init__(*args, **kwargs)

    def perform(self) -> DataSet:
        ziped = zip(*(op.perform() for op in self.previous_op))
        return map(self._perform_single, ziped)

    @property
    def markers(self):
        return [marker for op in self.previous_op for marker in op.markers]
    
    def _perform_single(self, ziped):
        res = {}
        for partial in ziped:
            res.update(partial)
        return res

    @property
    def compounded_markers_and_version_str(self):
        if self.previous_op is None:
            return []
        return [marker for op in self.previous_op 
                for marker in op.compounded_markers_and_version_str 
                if not any(requirement.compatible(marker[0]) for requirement in self.requires.values())]
    
    @property
    def ready(self):
        return all([op.ready for op in self.previous_op])

    def __call__(self, other: List["AbstractOp"]) -> "AbstractOp":
        self.previous_op = other
        for other_single_op in other:
            self._link_versions(other_single_op)
        return self

