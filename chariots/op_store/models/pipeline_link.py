from sqlalchemy import Column, Integer, ForeignKey

from ..models import db


class DBPipelineLink(db.Model):

    id = Column(Integer, primary_key=True)
    pipeline_id = Column(Integer, ForeignKey('db_pipeline.id'))
    upstream_op_id = Column(Integer, ForeignKey('db_op.id'), nullable=False)
    downstream_op_id = Column(Integer, ForeignKey('db_op.id'), nullable=True)
