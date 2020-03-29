from sqlalchemy import Column, Integer, ForeignKey


def build_validated_link_table(db):

    class ValidatedLink(db.Model):
        id = Column(Integer, primary_key=True)
        upstream_op_id = Column(Integer, ForeignKey(db.Op.id))
        downstream_op_id = Column(Integer, ForeignKey(db.Op.id))
        downstream_op_version_id = Column(Integer, ForeignKey(db.Version.id))

    return ValidatedLink
