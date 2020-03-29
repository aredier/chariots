from sqlalchemy import Column, Integer, ForeignKey


def build_pipeline_table(db):

    class Pipeline(db.Model):

        id = Column(Integer, primary_key=True)
        last_op_id = Column(Integer, ForeignKey(db.Op.id))

    return Pipeline
