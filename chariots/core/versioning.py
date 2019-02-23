"""
package that provides the signatures of each op
"""

import json
from typing import Text
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

class VersionedSignature:
    NotImplemented