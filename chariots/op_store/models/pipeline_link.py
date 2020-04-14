from sqlalchemy import Column, Integer, ForeignKey

from . import DBPipeline, DBOp
from ..models import db


class DBPipelineLink(db.Model):

    id = Column(Integer, primary_key=True)
    pipeline_id = Column(Integer, ForeignKey(DBPipeline.id))
    upstream_op_id = Column(Integer, ForeignKey(DBOp.id), nullable=False)
    downstream_op_id = Column(Integer, ForeignKey(DBOp.id), nullable=True)
