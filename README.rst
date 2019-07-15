========
chariots
========


.. image:: https://img.shields.io/pypi/v/chariots.svg
        :target: https://pypi.python.org/pypi/chariots

.. image:: https://img.shields.io/travis/aredier/chariots.svg
        :target: https://travis-ci.org/aredier/chariots

.. image:: https://readthedocs.org/projects/chariots/badge/?version=latest
        :target: https://chariots.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




chariots aims to be a complete framework to build and deploy versioned machine learning pipelines.

Chariots allow you to painlessly integrate semantic verisoning to your machine learning pipeline. For instance if one of your preprcessors's major version changes, a Chariot pipeline will not accept to be executed (prompting you to retrain the downstream nodes of your pipeline).

* Free software: GNU General Public License v3
* Documentation: https://chariots.readthedocs.io.

Getting Started: 30 seconds to Chariots:
----------------------------------------
in this section we will build a very small pipeline that counts the number it recieved a positive:

the atomic building blocks of chariots are the `Ops` those are the basic compute units of your pipeline.

first we create an op that returns one if it's input is positive and zero otherwise::

    from chariots.core import pipelines, ops, saving, versioning, nodes

    class IsInteresting(ops.AbstractOp):
        def __call__(self, input_number):
            return int(input_number>0)


we can then build another op that counts the number of signals it received from upstream. This op will have to be a `LoadableOp`as the counter needs to be persisted::

    class Counter(ops.LoadableOp):
        def __init__(self):
            self.count = 0

        def load(self, serialized_object: bytes):
            self.count = saving.JSONSerializer().deserialize_object(serialized_object)


        def serialize(self) -> bytes:
            return saving.JSONSerializer().serialize_object(self.count)

        def __call__(self, is_positive):
            self.count += is_positive
            return self.count


we can now build our first pipeline. A pipeline is collection of nodes linked together (a node usually wraps around an op)::

    pipe = pipelines.Pipeline([
        nodes.Node(IsInteresting(), input_nodes=["__pipeline_input__"],
                   output_node="is_pos"),
        nodes.Node(Counter(), input_nodes=["is_pos"],
                   output_node="__pipeline_output__")
    ], "hello_world")


once this is done, we can build an app to deploy our pipeline. This is an enhanced `Flask` app (meaning you can use and customize it in the same way)::

    from chariots.backend import app

    app = app.Chariot(pipelines=[pipe], path="/tmp/chariots", import_name="my_app")


we can than deploy it by running::

    $ flask

as you would with any flask app.

Once this is done we can query our pipeline using the Chariot `Client`::

    from chariots.backend import client

    c = client.Client()
    c.call_pipeline(pipe, pipeline_input=-2)

we can than save our counter by curling the address `http://127.0.0.1:5000/pipelines/hello_world/save` (integration in the client soon)

once this is done. if we want to change the logic of `IsInteresting` by ading a step, we need to add a `VersionedField` which will vhange its `affected_version`::

    class IsInteresting(ops.AbstractOp):

        step = versioning.VersionedField(0, affected_version=versioning.VersionType.MAJOR)

        def __call__(self, input_number):
            return int(input_number>0) * step

and when we redeploy, if we check if the pipeline loaded properly by curling `http://127.0.0.1:5000/pipelines/hello_world/health_check` (client integration coming soon) we will recieve::

    {"is_loaded": false}

and trying to execute this pipeline will fail (all other unafected pipelines will still work normally)

Features
--------

* versionable individual op
* easy pipeline building
* easy pipelines deployment

Comming Soon
------------

Some key features of Chariot are still in development and should be coming soon

* ML utils (implementation of ops for most popular ML libraries with adequate `Versionedfield`) for sklearn and keras at first
* Cloud integration (integration with cloud services to fetch and load models from)
* A CookieCutter to properly structure your ML project
* More examples (the example above is quite simple and we are going to write more of those to provide with some use cases and examples)

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypac
