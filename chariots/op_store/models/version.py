from sqlalchemy import Column, String, Integer, ForeignKey, DateTime

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

    def to_version_string(self):
        hash_str = '.'.join((self.major_hash, self.minor_hash, self.patch_hash))
        return '_'.join((hash_str, str(self.version_time)))
