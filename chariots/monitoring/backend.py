from datetime import datetime

from flask import Flask
from flask_cors import CORS
import graphene
from flask_graphql import GraphQLView
from graphene_sqlalchemy import SQLAlchemyObjectType, SQLAlchemyConnectionField
from sqlalchemy.orm import (scoped_session, sessionmaker, relationship,
                            backref)
from sqlalchemy.engine import Engine

from chariots.monitoring.monitoring_interface import MonitoringSeriesMetadata, SQL_BASE, create_default_dbs


class SeriesPoint(graphene.ObjectType):
    time = graphene.types.datetime.DateTime()
    op_version = graphene.String()
    data = graphene.JSONString()


class Series(SQLAlchemyObjectType):
    client = None
    class Meta:
        model = MonitoringSeriesMetadata

    points = graphene.List(SeriesPoint, version_srting=graphene.String())

    def resolve_points(self, info, version_srting=None):
        if Series.client is None:
            raise ValueError("influx db client not set canot get points")
        query_str = f"select * from {self.series_name}"
        if version_srting is not None:
            query_str += f" where version = '{version_srting}'"
        print(query_str)
        influx_data = Series.client.query(query_str + ";")
        if not influx_data:
            return []
        return [
            SeriesPoint(
                time=datetime.strptime(raw_point.pop("time").split(".")[0], "%Y-%m-%dT%H:%M:%S"),
                op_version=".".join(map(lambda subversion: subversion[:7],
                                        raw_point.pop("version", None).split( "."))),
                data=raw_point,
            )
            for raw_point in influx_data[(self.series_name, None)]
        ]


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
    cors = CORS(app, resources={r"/graphql/*": {"origins": "*"}})
    app.add_url_rule('/graphql',
                     view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True))
    app.teardown_appcontext(lambda _: db_session.remove())
    return app

app = create_app()





