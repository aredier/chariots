from flask import Flask

from chariots.op_store import models

import graphene
from graphene import relay
from flask_graphql import GraphQLView
from graphene_sqlalchemy import SQLAlchemyObjectType, SQLAlchemyConnectionField


class Op(SQLAlchemyObjectType):
    class Meta:
        model = models.DBOp
        interfaces = (relay.Node, )


class Version(SQLAlchemyObjectType):
    class Meta:
        model = models.DBVersion
        interfaces = (relay.Node, )


class PipelineLink(SQLAlchemyObjectType):
    class Meta:
        model = models.DBPipelineLink
        interfaces = (relay.Node, )


class ValidatedLink(SQLAlchemyObjectType):
    class Meta:
        model = models.DBValidatedLink
        interfaces = (relay.Node,)


class Pipeline(SQLAlchemyObjectType):
    class Meta:
        model = models.DBPipeline
        interfaces = (relay.Node,)


class Query(graphene.ObjectType):
    node = relay.Node.Field()

    op = relay.Node.Field(Op)
    ops = SQLAlchemyConnectionField(Op)

    version = relay.Node.Field(Version)
    versions = SQLAlchemyConnectionField(Version)

    pipelineLink = relay.Node.Field(PipelineLink)
    pipelineLinks = SQLAlchemyConnectionField(PipelineLink)




schema = graphene.Schema(query=Query)


class MetadataServer:
    def __init__(self, db_url='sqlite:///:memory:'):
        self.flask = Flask('OpStoreServer')
        self.flask.config['SQLALCHEMY_DATABASE_URI'] = db_url
        self.flask.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        self.db = models.db  # pylint: disable=invalid-name
        self.db.app = self.flask
        self.db.init_app(self.flask)
        self._init_routes()

    def _init_routes(self):
        self.flask.add_url_rule(
            '/graphql',
            view_func=GraphQLView.as_view(
                'graphql',
                schema=schema,
                graphiql=True  # for having the GraphiQL interface
            )
        )
