# pylint: disable=missing-module-docstring, missing-class-docstring, too-few-public-methods
from sqlalchemy import Column, Integer, ForeignKey, String
from sqlalchemy.orm import relationship

from ..models import db, DBPipelineLink


class DBPipeline(db.Model):

    id = Column(Integer, primary_key=True)
    pipeline_name = Column(String)

    links = relationship(DBPipelineLink, backref='pipeline')
