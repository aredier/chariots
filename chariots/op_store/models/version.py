# pylint: disable=missing-module-docstring, missing-class-docstring, too-few-public-methods
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime
from sqlalchemy.orm import relationship

from chariots import versioning
from ..models import db, DBValidatedLink


class DBVersion(db.Model):

    id = Column(Integer, primary_key=True)
    op_id = Column(Integer, ForeignKey('db_op.id'))
    version_time = Column(DateTime)

    major_hash = Column(String)
    major_version_number = Column(Integer)

    minor_hash = Column(String)
    minor_version_number = Column(Integer)

    patch_hash = Column(String)
    patch_version_number = Column(Integer)

    validated_downstream_links = relationship(DBValidatedLink, backref='upstream_version')

    def to_chariots_version(self):
        """converts DBVersion to equivalent `chariots.versioning.Version`"""
        return versioning.Version(self.major_hash, self.minor_hash, self.patch_hash, self.version_time)

    def to_version_string(self):
        """converts DBVersion to equivalent version string"""
        return str(self.to_chariots_version())
