"""
package that provides the signatures of each op
"""

import json
import time
from enum import Enum
from typing import Text
from hashlib import md5
from typing import Mapping
from typing import Optional
from typing import Hashable


class VersionType(Enum):
    PATCH = 1
    MINOR = 2
    MAJOR = 3


class SubVersion:
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

    def _update_time(self):
        """updates the last updated time stamp
        """

        self._last_update_time_stamp = time.time()
    
    def __eq__(self, other: "SubVersion") -> bool:
        return self.fields_hash ==  other.fields_hash

    def __gt__(self, other: "SubVersion") -> bool:
        return self.fields_hash !=  other.fields_hash and \
               self.last_update_time_stamp > other.last_update_time_stamp

    def __repr__(self):
        return f"{str(self._last_update_time_stamp)[:10]}.{self.fields_hash}"
    

class Version:
    """represents a full version (major, minor and patch)
    """

    def __init__(self):
        self.major = SubVersion()
        self.minor = SubVersion()
        self.patch = SubVersion()
    
    def __repr__(self):
        return f"<Version {self.major.display_number}.{self.minor.display_number}.{self.patch.display_number}>"
    
    def __eq__(self, other: "Version") -> bool:
        return self.major == other.major and self.minor == other.minor and self.patch == other.patch
    
    def __gt__(self, other: "Version") -> bool:
        return self.major > other.major or \
               (self.major == other.major and self.minor > other.minor) or  \
               (self.major == other.major and self.minor == other.minor and self.patch > other.patch)


class VersionField:
    """represents a version field: a field that will be attached to a subversion of a version 
    instance (major, minor patch)
    
    Raises:
        ValueError -- if the default factory and the default value are both set
    """

    def __init__(self, subversion: VersionType = VersionType.MINOR, default_value = None,
                 default_factory = None):
        if default_factory is not None and default_value is not None:
            raise ValueError("setting both the default value and the default_factory is wrong, choose please")
        self._inner_value = default_value or default_factory()
        self.subversion = subversion
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
        print(self._linked_subversion)
        self._inner_value = value
        self._update_version()
    
    def _update_version(self):
        """updates the parent subversion with itself
        """

        self._linked_subversion.update_fields(**{self._name: self._inner_value})


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
    version = Version()
    for name, value in cls.__dict__.items():
        if isinstance(value, VersionField):
            if value.subversion == VersionType.PATCH:
                value.link(version.patch, name)
            elif value.subversion == VersionType.MINOR:
                value.link(version.minor, name)
            elif value.subversion == VersionType.MAJOR:
                value.link(version.major, name)
    cls.version = version
