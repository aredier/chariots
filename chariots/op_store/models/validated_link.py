from sqlalchemy import Column, Integer, ForeignKey

from .op import DBOp
from .version import DBVersion
from ..models import db


class DBValidatedLink(db.Model):
    id = Column(Integer, primary_key=True)
    upstream_op_id = Column(Integer, ForeignKey(DBOp.id))
    downstream_op_id = Column(Integer, ForeignKey(DBOp.id))
    upstream_op_version_id = Column(Integer, ForeignKey(DBVersion.id))
