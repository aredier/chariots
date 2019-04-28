from flask import Flask
import graphene
from flask_graphql import GraphQLView
from graphene_sqlalchemy import SQLAlchemyObjectType, SQLAlchemyConnectionField
from sqlalchemy.orm import (scoped_session, sessionmaker, relationship,
                            backref)
from sqlalchemy.engine import Engine

from chariots.monitoring.monitoring_interface import MonitoringSeriesMetadata, SQL_BASE, create_default_dbs


class Series(SQLAlchemyObjectType):
    client = None
    class Meta:
        model = MonitoringSeriesMetadata

    point = graphene.List(graphene.types.json.JSONString)

    def resolve_point(self, info):
        if Series.client is None:
            raise ValueError("influx db client not set canot get points")
        return Series.client.query(f"select * from {self.series_name};") or [{
            "foo": f"select * from {self.series_name};",
            "bar": str(self.client)
        }]


class Query(graphene.ObjectType):
    series = graphene.Field(graphene.List(Series),
                            series_name=graphene.Argument(type=graphene.String, required=False))

    @staticmethod
    def resolve_series(args, info, series_name=None):
        query = Series.get_query(info)
        if series_name is not None:
            query = query.filter(MonitoringSeriesMetadata.series_name == series_name)
        return query.all()


def create_app(sql_engine: Engine = None, influx_db_client = None):
    default_engine, default_influx_client = create_default_dbs()
    sql_engine = sql_engine or default_engine
    influx_client = influx_db_client or default_influx_client
    Series.client = influx_client

    db_session = scoped_session(sessionmaker(autocommit=False,
                                             autoflush=False,
                                             bind=sql_engine))
    SQL_BASE.query = db_session.query_property()

    schema = graphene.Schema(query=Query)

    app = Flask("monitoring backend")
    app.add_url_rule('/graphql',
                     view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True))
    app.teardown_appcontext(lambda _: db_session.remove())
    return app

app = create_app()





