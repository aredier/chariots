from sqlalchemy import Column, String, Integer

from ..models import db


class DBOp(db.Model):
    id = Column(Integer, primary_key=True)
    op_name = Column(String)
