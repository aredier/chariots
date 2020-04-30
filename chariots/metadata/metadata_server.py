

from flask import Flask

from chariots import op_store

import graphene
from graphene import relay, String
from flask_graphql import GraphQLView
from graphene_sqlalchemy import SQLAlchemyObjectType, SQLAlchemyConnectionField

class MetadataServer:
    def __init__(self, db_url='sqlite:///:memory:'):
        self.flask = Flask('metadarta')
        self.flask.config['SQLALCHEMY_DATABASE_URI'] = db_url
        self.flask.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        self.db = op_store.models.db  # pylint: disable=invalid-name
        self.db.app = self.flask
        self.db.init_app(self.flask)
        self.schema = self._build_schema()
        self._init_routes()

    def _init_routes(self):
        self.flask.add_url_rule(
            '/graphql',
            view_func=GraphQLView.as_view(
                'graphql',
                schema=self.schema,
                graphiql=True  # for having the GraphiQL interface
            )
        )

    def _build_schema(self):
        class Op(SQLAlchemyObjectType):
            class Meta:
                model = op_store.models.DBOp
                filter_fields = ['op_name']
                interfaces = (relay.Node, )

        class Version(SQLAlchemyObjectType):
            class Meta:
                model = op_store.models.DBVersion
                interfaces = (relay.Node, )

        class PipelineLink(SQLAlchemyObjectType):
            class Meta:
                model = op_store.models.DBPipelineLink
                interfaces = (relay.Node, )

        class ValidatedLink(SQLAlchemyObjectType):
            class Meta:
                model = op_store.models.DBValidatedLink
                interfaces = (relay.Node,)

        class Pipeline(SQLAlchemyObjectType):
            class Meta:
                model = op_store.models.DBPipeline
                interfaces = (relay.Node,)

        class Query(graphene.ObjectType):
            node = relay.Node.Field()

            ops = SQLAlchemyConnectionField(Op, name=String())
            versions = SQLAlchemyConnectionField(Version)
            pipelines = SQLAlchemyConnectionField(Pipeline)

            op = relay.Node.Field(Op)
            pipeline = relay.Node.Field(Pipeline)
            version = relay.Node.Field(Version)
            pipelineLink = relay.Node.Field(PipelineLink)
            validatedLink = relay.Node.Field(ValidatedLink)

            def resolve_ops(self, info, **args):
                base_query = Op.get_query(info)
                if 'name' in args:
                    base_query = base_query.filter(op_store.models.DBOp.op_name == args['name'])
                return base_query.all()

        return graphene.Schema(query=Query)
