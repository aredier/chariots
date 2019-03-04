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


class Signature:
    """
    a `Signature` represents an op, it identifies it from version to version
    """
    
    def __init__(self, name: Text, identifiers: Optional[Mapping[Text, Text]] = None):
        self.name = name
        self._identifiers = identifiers or {}
    
    @property
    def identifier(self):
        return {"name": self.name, **self._identifiers}
    
    def add_fields(self, **kwargs):
        self._identifiers = self._identifiers.update(kwargs)

    @property
    def checksum(self):
        """
        the checksum of an op
        """
        return hash(json.dumps(self.identifier))
    
    def matches(self, other: "Signature"):
        return self.checksum == other.checksum

    def __repr__(self):
        return f"<Signature of {self.name}.{self.checksum}>"

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
    def fields_hash(self):
        res_hash = md5()
        for value in self._fields.values():
            res_hash.update(str(hash(value)).encode("utf-8"))
        return res_hash.hexdigest()
    
    @property
    def last_update_time_stamp(self):
        return self._last_update_time_stamp

    def update_fields(self, **fields):
        old_hash = self.fields_hash
        self._fields.update(fields)
        self._update_time()
        if self.fields_hash != old_hash:
            self.display_number += 1

    def _update_time(self):
        self._last_update_time_stamp = time.time()
    
    def __eq__(self, other: "SubVersion") -> bool:
        return self.fields_hash ==  other.fields_hash

    def __gt__(self, other: "SubVersion") -> bool:
        return self.last_update_time_stamp > other.last_update_time_stamp

    def __repr__(self):
        return f"{str(self._last_update_time_stamp)[:10]}.{self.fields_hash}"
    
class Version:
    def __init__(self):
        self.major = SubVersion()
        self.minor = SubVersion()
        self.patch = SubVersion()
    
    def __repr__(self):
        return f"<Version {self.major.display_number}.{self.minor.display_number}.{self.patch.display_number}>"
    
    def __eq__(self, other: "Version") -> bool:
        return self.major == other.major and self.minor == other. major
    
    def __ge__(self, other: "Version") -> bool:
        return self.major >= other.major and self.minor >= other.minor and self.patch >= other.minor

class VersionField:
    def __init__(self, subversion: VersionType = VersionType.MINOR, default_value = None,
                 default_factory = None):
        if default_factory is not None and default_value is not None:
            raise ValueError("setting both the default value and the default_factory is wrong, choose please")
        self._inner_value = default_value or default_factory()
        self.subversion = subversion
        self._linked_subversion: SubVersion = None
        self._name = None

    @property
    def value(self):
        return self._inner_value
    
    def link(self, version: SubVersion, name: Text):
        self._linked_subversion = version
        self._name = name
        self._update_version()

    def set(self, value):
        if self._linked_subversion is None:
            raise ValueError("cannot set the value of an unlinked version field")
        self._inner_value = value
        self._update_version()
    
    def _update_version(self):
        self._linked_subversion.update_fields(**{self._name: self._inner_value})

def _extract_versions(cls):
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
