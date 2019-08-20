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
in this section we will build a very small pipeline that tries to classify iris species from their phisilogical properties:

the atomic building blocks of chariots are the `Ops` those are the basic compute units of your pipeline.

first we can create ops that return the training data::

   class IrisX(AbstractOp):

        fields_of_interest = VersionedField(['sepal length (cm)', 'sepal width (cm)', 'petal length (cm)',
           'petal width (cm)'], VersionType.MINOR)

        def __call__(self):
            iris = datasets.load_iris()

            data1 = pd.DataFrame(data=iris['data'],
                                 columns= iris['feature_names'])
            return data1.loc[:, self.fields_of_interest]

    class IrisY(AbstractOp):

        def __call__(self):
            iris = datasets.load_iris()
            return iris["target"]


you will notice that `IrisX` has a special `VersionedField` this means that each time this field gets updated, the version of the op changes

we can then build the machine learning part of our pipleine::

    class PCAOp(SKUnsupervisedModel):

        model_class = VersionedField(PCA, VersionType.MAJOR)
        training_update_version = VersionType.MAJOR
        model_parameters = VersionedFieldDict(VersionType.MAJOR, {
            "n_components": 2,
        })

    class RandomForestOp(SKSupervisedModel):
        model_class = VersionedField(RandomForestClassifier, VersionType.MAJOR)
        training_update_version = VersionType.MAJOR
        model_parameters = VersionedFieldDict(VersionType.MINOR, {
            "n_estimators" : VersionedField(5, VersionType.MAJOR),
            "max_depth": 2
        })



we can now build our first pipeline. A pipeline is collection of nodes linked together (a node usually wraps around an op)::

    train = Pipeline([
        Node(IrisX(), output_node="x_raw"),
        Node(PCAOp(MLMode.FIT_PREDICT), input_nodes=["x_raw"], output_node="x_transformed"),
        Node(IrisY(), output_node="y"),
        Node(RandomForestOp(MLMode.FIT), input_nodes=["x_transformed", "y"])
    ], "train")


we also have to create a pipeline for prediction::

    predict = Pipeline([
        Node(PCAOp(MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_node="x_transformed"),
        Node(RandomForestOp(MLMode.PREDICT), input_nodes=["x_transformed"], output_node="__pipeline_output__")
    ], "predict")

we can now use our pipleines, save and load them::

    train(SequentialRunner())
    store = OpStore(FileSaver("/tmp/chariots_test"))
    train.save(store)
    predict.load(store)
    predict(SequentialRunner(), [[5.4, 3.5, 1.4, .2]])

here we are running the training pipeline, persisting the ops of our pipleine and reloading the equivalent ops in the prediction pipeline and running a prediction
One of the advantage of chariots is to enforce your versioning. meaning if you change the `n_components` of your PCA, and try to reload an old pipeline, this will raise a `VersionError`, avoiding undefined behavior.

once your are done with prototyping locally (or if you don't want to redefine your runners and savers each time), you can deploy a Chariot app.
this is flask app built to handle Chariot pipleines::

    from chariots.backend import app

    app = Chariot(app_pipelines=[train, predict], path="/tmp/chariots", import_name="my_app")


we can than deploy it by running::

    $ flask

as you would with any flask app.

Once this is done we can query our pipeline using the Chariot `Client`::

    from chariots.backend import client

    c = Client()
    c.call_pipeline(train)
    c.save_pipeline(train)
    c.load_pipeline(predict)
    c.call_pipeline(predict, pipeline_input=[[5.4, 3.5, 1.4, .2]])

Features
--------

* versionable individual op
* easy pipeline building
* easy pipelines deployment
* ML utils (implementation of ops for most popular ML libraries with adequate `Versionedfield`) for sklearn and keras at first
* A CookieCutter to properly structure your Chariots project

Comming Soon
------------

Some key features of Chariot are still in development and should be coming soon

* Cloud integration (integration with cloud services to fetch and load models from)
* More examples (the example above is quite simple and we are going to write more of those to provide with some use cases and examples)

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.
`audreyr/cookiecutter-pypackage`_'s project is also the basis of the Chariiots project template

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypac
