from sqlalchemy import Column, String, Integer, ForeignKey


def build_version_table(db):

    class Version(db.Model):

        id = Column(Integer, primary_key=True)
        op_id = Column(Integer, ForeignKey(db.Op.id))

        major_hash = Column(String)
        major_number = Column(Integer)

        minor_hash = Column(String)
        minor_number = Column(Integer)

        patch_hash = Column(String)
        patch_number = Column(Integer)

    return Version
