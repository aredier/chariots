from sqlalchemy import Column, String, Integer


def build_op_table(db):

    class Op(db.Model):
        id = Column(Integer, primary_key=True)
        op_name = Column(String)

    return Op
