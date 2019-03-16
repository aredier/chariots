"""
package that provides the signatures of each op
"""
import json
import time
import copy
import uuid
from abc import ABC
from abc import abstractproperty
from enum import Enum
from typing import Text
from hashlib import md5
from typing import Mapping
from typing import Optional
from typing import Hashable

VERSIONING_PRE = "_versioned_"
class VersionType(Enum):
    PATCH = 1
    MINOR = 2
    MAJOR = 3


class AbstractSubversion(ABC):
    """subversion interface
    """

    @abstractproperty
    def last_update_time_stamp(self) -> float:
        pass

    @abstractproperty
    def fields_hash(self) -> Text:
        pass

    def __eq__(self, other: "AbstractSubversion") -> bool:
        return self.fields_hash ==  other.fields_hash

    def __gt__(self, other: "AbstractSubversion") -> bool:
        return self.fields_hash !=  other.fields_hash and \
               self.last_update_time_stamp > other.last_update_time_stamp

    def __repr__(self):
        return f"{self.last_update_time_stamp}_{self.fields_hash}"
    
    def __hash__(self):
        return hash(str(self))


class SubVersion(AbstractSubversion):
    """
    represents part of the version (major, minor, ...)
    a subversion is considered to be equal to another if their fields are equal and greater if its
    fields are different and it was last updated after the other
    the display_number will not change the underlying logic
    """
    def __init__(self, display_number: int = 0):
        self._fields = {}
        self._update_time()
        self.display_number = display_number
        self.linked_subversions = set()
        self._unique_identifier = uuid.uuid1()
    
    @property
    def fields_hash(self) -> Text:
        """hash representing the state of the of the Subversion field
        
        Returns:
            [Text] -- the hash of the field
        """
        res_hash = md5()
        for value in self._fields.values():
            res_hash.update(str(hash(value)).encode("utf-8"))
        return res_hash.hexdigest()
    
    @property
    def last_update_time_stamp(self) -> float:
        """the time at which the fields were last updated
        
        Returns:
            float -- the time stamp 
        """
        return self._last_update_time_stamp

    def update_fields(self, **fields):
        """Updates the fields of the subversion with paraometers
        the fields' value must be hashable
        """
        
        old_hash = self.fields_hash
        self._fields.update(fields)
        self._update_time()
        if self.fields_hash != old_hash:
            self.display_number += 1
        
        # TODO this looks like a rocky implementation
        for linkded_subversion in self.linked_subversions:
            linkded_subversion.update_fields(**{
                str(self._unique_identifier): self.last_update_time_stamp
                })

    def _update_time(self):
        """updates the last updated time stamp
        """
        self._last_update_time_stamp = time.time()
    
    def link(self, other: "SubVersion"):
        """links this version to the next, this means that whenever `other` gets updated
        this version will evolve as well
        
        Arguments:
            other {SubVersion} -- the version to be linked to 
        """
        print(self)
        other.linked_subversions.add(self)
        other.update_fields()



class SubversionString(AbstractSubversion):
    """this class represents a subversion but is not able to actually be updated,
    it is supposed to be used to deprecate old ops without having to actually load them
    """

    def __init__(self, version_string: Text):
        self._last_update_time_stamp, self._fields_hash = version_string.split("_")
        self._last_update_time_stamp = float(self._last_update_time_stamp)

    @property
    def last_update_time_stamp(self) -> float:
        return self._last_update_time_stamp

    @property 
    def fields_hash(self) -> Text:
        return self._fields_hash
    

class Version:
    """represents a full version (major, minor and patch)
    """

    def __init__(self):
        self.major = SubVersion()
        self.minor = SubVersion()
        self.patch = SubVersion()
    
    @classmethod
    def parse(cls, version_string: Text) -> "Version":
        res = cls()
        res.major, res.minor, res.patch = map(SubversionString, version_string.split("-"))
        return res
    
    def __repr__(self):
        return f"{self.major}-{self.minor}-{self.patch}"
    
    def __eq__(self, other: "Version") -> bool:
        return self.major == other.major and self.minor == other.minor and self.patch == other.patch
    
    def __gt__(self, other: "Version") -> bool:
        return self.major > other.major or \
               (self.major == other.major and self.minor > other.minor) or  \
               (self.major == other.major and self.minor == other.minor and self.patch > other.patch)


class _VersionField:
    """represents a version field: a field that will be attached to a subversion of a version 
    instance (major, minor patch)
    
    Raises:
        ValueError -- if the default factory and the default value are both set
    """

    def __init__(self, default_value = None, default_factory = None):
        if default_factory is not None and default_value is not None:
            raise ValueError("setting both the default value and the default_factory is wrong, choose please")
        self._inner_value = default_value or default_factory()
        self._linked_subversion: SubVersion = None
        self._name = None

    @property
    def value(self) -> Hashable:
        """getter for the value of the field
        
        Returns:
            Hashable -- the value
        """
        return self._inner_value
    
    def link(self, sub_version: SubVersion, name: Text):
        """links a field to it's subversion
        
        Arguments:
            sub_version {SubVersion} -- the subversion to link to
            name {Text} -- the name of the field in this subversion (this level of introspection is
            needed for the field to be able to tell it's version who it is when update time comes)
        """
        self._linked_subversion = sub_version
        self._name = name
        self._update_version()

    def set(self, value: Hashable):
        """sets the inner value of the field and updates linked subversion
        
        Arguments:
            value {Hashable} -- the value to set
        
        Raises:
            ValueError -- if the op is not linked to a version prior to seting
        """
        if self._linked_subversion is None:
            raise ValueError("cannot set the value of an unlinked version field")
        self._inner_value = value
        self._update_version()
    
    def _update_version(self):
        """updates the parent subversion with itself
        """
        if isinstance(self._inner_value, dict):
            self._linked_subversion.update_fields(**self._inner_value)
            return 
        self._linked_subversion.update_fields(**{self._name: self._inner_value})


class VersionField:
    """class that represents an uninstantiated (but prepared) versioned field
    """
    
    def __init__(self, subversion: VersionType = VersionType.MINOR, default_value = None,
                 default_factory = None):
        if default_factory is not None and default_value is not None:
            raise ValueError("setting both the default value and the default_factory is wrong, choose please")
        self.default_value = default_value
        self.default_factory = default_factory
        self.subversion = subversion
    
    def spawn(self) -> _VersionField:
        """creates a new instance of _VersionField corresponding to this instance's parameters
        
        Returns:
            _VersionField -- the resulting versioned field
        """

        return _VersionField(default_value=self.default_value, default_factory=self.default_factory)


def _extract_versioned_fields(cls):
    """function to extract the `VersionField`s that might be present as class attributes of a class
    and creates an additional `version` class attribute with all the correcly linked versions.
    
    BEWARE calling this function in the `__new__` method of a clas doesn't unwrap the `VersionField`
    attributes. Those will still be of class `VersionField` hence to accesss or set those values, 
    you will need to do something in the likes of: 
    ``` 
    my_object.my_field.value
    ```
    and
    ```
    my_object.my_field.set(my_value)
    ```
    in order to have a more seemless integration you should look at how `BasicOp` overides the
    `__setattr__` and `__getattribute__` method
    """
    all_class_attributes = {}
    for parent_class in cls.__mro__[::-1]:
        all_class_attributes.update(parent_class.__dict__)
    version = Version()
    for name, value in all_class_attributes.items():
        if isinstance(value, VersionField):
            instance = value.spawn()
            setattr(cls, VERSIONING_PRE + name, instance)
            if value.subversion == VersionType.PATCH:
                instance.link(version.patch, name)
            elif value.subversion == VersionType.MINOR:
                instance.link(version.minor, name)
            elif value.subversion == VersionType.MAJOR:
                instance.link(version.major, name)
    return version
