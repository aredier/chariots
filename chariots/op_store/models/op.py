# pylint: disable=missing-module-docstring, missing-class-docstring, too-few-public-methods
from sqlalchemy import Column, String, Integer

from ..models import db


class DBOp(db.Model):
    id = Column(Integer, primary_key=True)
    op_name = Column(String)

    def __repr__(self):
        return 'DBOp(id={}, op_name={})'.format(self.id, self.op_name)
