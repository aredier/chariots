# pylint: disable=missing-module-docstring, missing-class-docstring, too-few-public-methods
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship

from ..models import db, DBVersion, DBPipelineLink, DBValidatedLink


class DBOp(db.Model):
    id = Column(Integer, primary_key=True)
    op_name = Column(String)

    versions = relationship(DBVersion, backref='op')
    upstream_links = relationship(DBPipelineLink, primaryjoin='DBOp.id==DBPipelineLink.downstream_op_id',
                                  backref='downstream_op')
    downstream_links = relationship(DBPipelineLink, primaryjoin='DBOp.id==DBPipelineLink.upstream_op_id',
                                    backref='upstream_op')
    upstream_validated_links = relationship(DBValidatedLink, primaryjoin='DBOp.id==DBValidatedLink.downstream_op_id',
                                            backref='downstream_op')
    downstream_validated_links = relationship(DBValidatedLink, primaryjoin='DBOp.id==DBValidatedLink.upstream_op_id',
                                              backref='upstream_op')

    def __repr__(self):
        return 'DBOp(id={}, op_name={})'.format(self.id, self.op_name)
