# pylint: disable=missing-module-docstring, missing-class-docstring, too-few-public-methods
from sqlalchemy import Column, Integer, ForeignKey, String

from .op import DBOp
from ..models import db


class DBPipeline(db.Model):

    id = Column(Integer, primary_key=True)
    pipeline_name = Column(String)
