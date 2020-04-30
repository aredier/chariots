"""op store server file"""
# pylint: disable=no-member

import base64

from flask import Flask, request, jsonify
from flask_migrate import Migrate
from sqlalchemy.orm import aliased

from .models import db, DBPipelineLink
from .models.version import DBVersion
from .models.op import DBOp
from .models.validated_link import DBValidatedLink
from .models.pipeline import DBPipeline
from ..versioning import Version


class OpStoreServer:
    """
    The OpStore Server is the server that handles Saving and loading the different ops as well as keeping track
    of all the existing versions of each op.

    To Create a server, you need to provide it with a saver (to know how to persist the Ops) and db_url (a sqlalchemy
    compatible url for the server to connect to the url)

    .. testsetup::

        >>> import tempfile
        >>> import shutil
        >>> my_url = 'foo'
        >>> saver_path = tempfile.mkdtemp()

    .. doctest::

        >>> from chariots.op_store import savers
        ...
        >>> saver = savers.FileSaver(saver_path)
        >>> op_store_client = OpStoreServer(saver=saver, db_url=my_url)

    The OpStore is created around a Flask app that you can access through the `.flask` attribute:

    .. doctest::

        >>> op_store_client.flask
        <Flask 'OpStoreServer'>

    You can also access the `.db` and `.migrate` to control the db and potential migration (if newer versions
    of Chariots change the schema of the OpStore database for instance

    .. testsetup::
        >>> shutil.rmtree(saver_path)

    Since this is a server, its public methods should not be accessed directly but instead through http (using the
    op store client)

    The OpStore is mostly used by the Pipelines and the nodes at saving time to:

    - persist the ops that they have updated
    - register new versions
    - register links between different ops and different versions that are valid (for instance this versions of the PCA
      is valid for this new version of the RandomForest

    and at loading time to:

    - check latest available version of an op
    - check if this version is valid with the rest of the pipeline
    - recover the bytes of the latest version if it is valid

    the OpStore identifies op's by there name (usually a snake case of the Class of your op) so changing this name
    (or changing the class name) might make it hard to recover the metadata and serialized bytes of the Ops.

    :param saver: the saver to use to persist the ops.
    :param db_url: the URL of the database (where all the versions and pipeline informations are stored)
    """

    def __init__(self, saver, db_url='sqlite:///:memory:'):
        self.flask = Flask('OpStoreServer')
        self.flask.config['SQLALCHEMY_DATABASE_URI'] = db_url
        self.flask.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        self.db = db  # pylint: disable=invalid-name
        self.db.app = self.flask
        self.db.init_app(self.flask)
        self.migrate = Migrate(self.flask, self.db)
        self._saver = saver
        self._init_routes()

    @property
    def _session(self):
        return self.db.session

    def get_all_versions_of_op(self):
        """
        returns all the available versions of an op ever persisted in the OpGraph (or any Opgraph using the same
        _meta.json)
        """

        desired_op_name = request.json['desired_op_name']

        query = (self._session
                 .query(DBOp, DBVersion)
                 .filter(DBOp.op_name == desired_op_name)
                 .filter(DBOp.id == DBVersion.op_id))
        all_versions = {
            query_result[1].to_version_string()
            for query_result in query
        }
        return jsonify({
            'op_name': desired_op_name,
            'all_versions': list(all_versions)
        } if all_versions else None)

    def get_validated_links(self):
        """
        gets all the validated links (versions that works) between an upstream op and a downstream op (if none
        exist, `None` is returned)
        """

        downstream_op_name = request.json['downstream_op_name']
        upstream_op_name = request.json['upstream_op_name']

        downstream_op_table = aliased(DBOp)
        upstream_op_table = aliased(DBOp)
        query = (self._session.query(downstream_op_table, upstream_op_table, DBValidatedLink, DBVersion)
                 .filter(DBValidatedLink.downstream_op_id == downstream_op_table.id)
                 .filter(downstream_op_table.op_name == downstream_op_name)
                 .filter(DBValidatedLink.upstream_op_id == upstream_op_table.id)
                 .filter(upstream_op_table.op_name == upstream_op_name)
                 .filter(DBValidatedLink.upstream_op_version_id == DBVersion.id))
        query = list(query)
        return jsonify({
            'downstream_op_name': downstream_op_name,
            'upstream_op_name': upstream_op_name,
            'upstream_versions': list({query_result[3].to_version_string() for query_result in query}),
        } if query else None)

    def get_op_bytes_for_version(self):
        """
        loads the persisted bytes of op for a specific version
        """
        desired_op_name = request.json['desired_op_name']
        version = Version.parse(request.json['version'])
        path = self._build_op_path(desired_op_name, version)
        return jsonify({
            'op_name': desired_op_name,
            'op_version': str(version),
            'bytes': base64.b64encode(self._saver.load(path=path)).decode('utf-8')
        })

    @staticmethod
    def _build_op_path(op_name: str, version: Version) -> str:
        """
        builds the path an op should be persisted at given it's version

        :param op_name: the name of the op to build the path for
        :param version: th the version of that op
        :return: the path at which to save
        """

        return '/models/{}/{}'.format(op_name, str(version))

    def save_op_bytes(self):
        """
        saves op_bytes of a specific op to the path /models/<op name>/<version>.

        the version that is used here is the node version (and not the op_version) as nodes might be able to modify
        some behaviors of the versioning of their underlying op
        """
        op_name = request.json['op_name']
        db_op = self._get_db_op(op_name=op_name)
        if db_op is None:
            raise ValueError('op {} not registered please register before saving op_bytes'.format(op_name))
        version = Version.parse(request.json['version'])
        db_version = self._get_db_version(version, db_op.id)
        if not db_version:
            raise ValueError('version {} not registered for {} please register'
                             ' op version before saving'.format(version, op_name))
        op_bytes = base64.b64decode(request.json['bytes'].encode('utf-8'))
        path = self._build_op_path(db_op.op_name, version=db_version.to_chariots_version())
        self._saver.save(serialized_object=op_bytes, path=path)
        return jsonify({})

    def register_valid_link(self):
        """
        registers a link between an upstream and a downstream op. This means that in future relaods the downstream op
        will whitelist this version for this upstream op
        """
        downstream_op_name = request.json['downstream_op_name']
        upstream_op_name = request.json['upstream_op_name']
        upstream_op_version = Version.parse(request.json['upstream_op_version'])

        upstream_op_id = self.get_or_register_db_op(upstream_op_name).id
        upstream_version_id = self.get_or_register_db_version(version=upstream_op_version, op_id=upstream_op_id).id
        if downstream_op_name is None:
            return jsonify({})
        downstream_op_id = self.get_or_register_db_op(downstream_op_name).id
        exists_query = self._session.query(DBValidatedLink).filter(
            DBValidatedLink.upstream_op_id == upstream_op_id,
            DBValidatedLink.downstream_op_id == downstream_op_id,
            DBValidatedLink.upstream_op_version_id == upstream_version_id,
        )
        if exists_query.one_or_none() is not None:
            return jsonify({})
        validated_link = DBValidatedLink(
            upstream_op_id=upstream_op_id,
            downstream_op_id=downstream_op_id,
            upstream_op_version_id=upstream_version_id
        )

        self._session.add(validated_link)
        self._session.commit()

        return jsonify({})

    def _get_db_op(self, op_name: str):
        return self._session.query(DBOp).filter(DBOp.op_name == op_name).one_or_none()

    def get_or_register_db_op(self, op_name: str) -> DBOp:
        """
        creates the db op corresponding to op name if it doesn't exist, otherwise returns the existing one
        :param op_name: the name of the op to look for
        :return: the created op
        """
        db_op = self._get_db_op(op_name)
        if db_op is not None:
            return db_op
        db_op = DBOp(op_name=op_name)
        self._session.add(db_op)
        self._session.commit()
        return db_op

    def _get_db_version(self, version: Version, op_id: int):
        return (self._session.query(DBVersion)
                .filter(DBVersion.op_id == op_id)
                .filter(DBVersion.major_hash == version.major)
                .filter(DBVersion.minor_hash == version.minor)
                .filter(DBVersion.patch_hash == version.patch)
                ).one_or_none()

    def get_or_register_db_version(self, version: Version, op_id: int) -> DBVersion:
        """
        creates the db version corresponding to version if it does not exist yet. Otherwise creates it

        :param version: the version ti look for
        :param op_id: the id of the op this version is attached to
        :return: the DBVersion
        """
        db_version = self._get_db_version(version, op_id)
        if db_version is not None:
            return db_version

        major_version_number, minor_version_number, patch_version_number = self._get_version_numbers(version, op_id)
        db_version = DBVersion(
            op_id=op_id,
            version_time=version.creation_time,
            major_hash=version.major,
            major_version_number=major_version_number,
            minor_hash=version.minor,
            minor_version_number=minor_version_number,
            patch_hash=version.patch,
            patch_version_number=patch_version_number,
        )
        self._session.add(db_version)
        self._session.commit()
        return db_version

    def _get_version_numbers(self, version: Version, op_id):
        last_op_versions = self._session.query(
            DBVersion
        ).filter(DBVersion.op_id == op_id).order_by(
            DBVersion.major_version_number, DBVersion.minor_version_number, DBVersion.patch_version_number
        ).limit(1).one_or_none()
        if not last_op_versions:
            return 1, 0, 0
        if last_op_versions.major_hash != version.major:
            return last_op_versions.major_version_number + 1, 0, 0
        if last_op_versions.minor_hash != version.minor:
            return last_op_versions.major_version_number, last_op_versions.minor_version_number + 1, 0
        if last_op_versions.patch_hash != version.patch:
            return (last_op_versions.major_version_number, last_op_versions.minor_version_number,
                    last_op_versions.patch_version_number + 1)
        return (last_op_versions.major_version_number, last_op_versions.minor_version_number,
                last_op_versions.patch_version_number)

    def pipeline_exists(self):
        """endpoint to check if the pipeline exists"""
        pipeline_name = request.json['pipeline_name']
        return jsonify({
            'exists': (self._session
                       .query(DBPipeline)
                       .filter(DBPipeline.pipeline_name == pipeline_name)
                       .one_or_none() is not None)
        })

    def register_new_pipeline(self):
        """endpoint ot register a new pipeline"""

        pipeline_name = request.json['pipeline_name']
        pipeline_links = request.json['pipeline_links']

        db_pipeline = DBPipeline(
            pipeline_name=pipeline_name,
        )
        self._session.add(db_pipeline)
        self._session.commit()
        for upstream_node_name, downstream_node_name in pipeline_links:
            upstream_op_id = self.get_or_register_db_op(upstream_node_name).id
            downstream_op_id = self.get_or_register_db_op(downstream_node_name).id if downstream_node_name else None
            self._session.add(
                DBPipelineLink(pipeline_id=db_pipeline.id, upstream_op_id=upstream_op_id,
                               downstream_op_id=downstream_op_id)
            )
            self._session.commit()

        return jsonify({})

    def _init_routes(self):

        self.flask.add_url_rule(
            '/v1/get_all_versions_of_op',
            'get_all_versions_of_op',
            self.get_all_versions_of_op,
            methods=['POST']
        )

        self.flask.add_url_rule(
            '/v1/get_validated_links',
            'get_validated_links',
            self.get_validated_links,
            methods=['POST']
        )

        self.flask.add_url_rule(
            '/v1/get_op_bytes_for_version',
            'get_op_bytes_for_version',
            self.get_op_bytes_for_version,
            methods=['POST']
        )

        self.flask.add_url_rule(
            '/v1/save_op_bytes',
            'save_op_bytes',
            self.save_op_bytes,
            methods=['POST']
        )

        self.flask.add_url_rule(
            '/v1/register_valid_link',
            'register_valid_link',
            self.register_valid_link,
            methods=['POST']

        )

        self.flask.add_url_rule(
            '/v1/pipeline_exists',
            'pipeline_exists',
            self.pipeline_exists,
            methods=['POST']

        )

        self.flask.add_url_rule(
            '/v1/register_new_pipeline',
            'register_new_pipeline',
            self.register_new_pipeline,
            methods=['POST']

        )
