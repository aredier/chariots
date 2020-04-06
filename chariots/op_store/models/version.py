# pylint: disable=missing-module-docstring, missing-class-docstring, too-few-public-methods
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime

from chariots import versioning
from .op import DBOp
from ..models import db


class DBVersion(db.Model):

    id = Column(Integer, primary_key=True)
    op_id = Column(Integer, ForeignKey(DBOp.id))
    version_time = Column(DateTime)

    major_hash = Column(String)
    major_version_number = Column(Integer)

    minor_hash = Column(String)
    minor_version_number = Column(Integer)

    patch_hash = Column(String)
    patch_version_number = Column(Integer)

    def to_chariots_version(self):
        """converts DBVersion to equivalent `chariots.versioning.Version`"""
        return versioning.Version(self.major_hash, self.minor_hash, self.patch_hash, self.version_time)

    def to_version_string(self):
        """converts DBVersion to equivalent version string"""
        return str(self.to_chariots_version())
