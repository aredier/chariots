import pytest
import numpy as np
from sklearn.naive_bayes import MultinomialNB

from chariots.core.markers import Matrix
from chariots.core.markers import Marker
from chariots.core.ops import BaseOp
from chariots.core.ops import Split
from chariots.core.ops import Merge
from chariots.training.trainable_pipeline import TrainablePipeline
from chariots.core.taps import DataTap
from chariots.training.sklearn import SingleFitSkSupervised
from chariots.training.sklearn import SingleFitSkTransformer

TextList = Marker.new_marker()
YMarker = Matrix.new_marker()


class TextVector(Matrix):

    def compatible(self, other):
        return isinstance(other, TextVector)


class YTrue(BaseOp):
    requires = {"text": TextList()}
    markers = YMarker((None, 1))

    def _main(self, text):
        return [int(sent[-1] == "r") for sent in text]

@pytest.fixture
def count_vectorizer():
    return SingleFitSkTransformer.factory(
        x_marker = TextList(),
        model_cls = MultinomialNB, 
        y_marker = TextVector(()),
        name = "naive_baise"
) 


@pytest.fixture
def naive_baise_op():
    return SingleFitSkSupervised.factory(
        x_marker = TextVector(()),
        model_cls = MultinomialNB, 
        y_marker = YMarker((None, 1)),
        name = "naive_baise"
    )



def test_sklearn_training(count_vectorizer, naive_baise_op):
    sentences = [
        "That there’s some good in this world, Mr. Frodo… and it’s worth fighting for",
        "A day may come when the courage of men fails… but it is not THIS day"
    ]
    train_size = 32
    data = DataTap(iter([np.random.choice(sentences, train_size, replace=True)]), TextList()) 
    x_train, y_train = Split(2)(data)
    y_train = YTrue()(y_train)

    model = TrainablePipeline()
    vectorizer = count_vectorizer()
    model.add(vectorizer)
    naive_baise = naive_baise_op()
    model.add(naive_baise)

    model.fit(Merge()([x_train, y_train]))

    test_data = DataTap(iter([[sentences[0] for _ in range(train_size)] for i in range(2)]),
                        TextList())
    x_test, y_test = Split(2)(data)
    y_test = YTrue()(y_test)

    pred = model(y_test)
    for res in pred.perform():
        res.should.be.a(dict)
        res.should.have.key(naive_baise.markers[0]).being(1)


# def test_sklearn_persistance():
#     pass


# def test_sklearn_persisted_major_deprecation():
#     pass


# def test_sklearn_persisted_minor_non_deprecation():
#     # TODO implement
#     pass