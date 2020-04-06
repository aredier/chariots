from sqlalchemy import Column, String, Integer

from ..models import db


class DBOp(db.Model):
    id = Column(Integer, primary_key=True)
    op_name = Column(String)

    def __repr__(self):
        return 'DBOp(id={}, op_name={})'.format(self.id, self.op_name)
