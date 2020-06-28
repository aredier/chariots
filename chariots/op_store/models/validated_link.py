# pylint: disable=missing-module-docstring, missing-class-docstring, too-few-public-methods
from sqlalchemy import Column, Integer, ForeignKey

from ..models import db


class DBValidatedLink(db.Model):
    id = Column(Integer, primary_key=True)
    upstream_op_id = Column(Integer, ForeignKey('db_op.id'))
    downstream_op_id = Column(Integer, ForeignKey('db_op.id'))
    upstream_op_version_id = Column(Integer, ForeignKey('db_version.id'))

    def __repr__(self):
        return 'DBValidatedLink(id={}, upstream_op_id={}, downstream_op_id={}, upstream_op_version_id={})'.format(
            self.id, self.upstream_op_id, self.downstream_op_id, self.upstream_op_version_id)
