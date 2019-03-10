import pytest
import numpy as np
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import CountVectorizer

from chariots.core.markers import Matrix
from chariots.core.markers import Marker
from chariots.core.ops import BaseOp
from chariots.core.ops import Split
from chariots.core.ops import Merge
from chariots.core.saving import FileSaver
from chariots.core.taps import DataTap
from chariots.training.sklearn import SingleFitSkSupervised
from chariots.training.sklearn import SingleFitSkTransformer
from chariots.training.trainable_pipeline import TrainablePipeline

TextList = Marker.new_marker()
YMarker = Matrix.new_marker()


class TextVector(Matrix):

    def compatible(self, other):
        return isinstance(other, TextVector)


class YTrue(BaseOp):
    requires = {"text": TextList()}
    markers = [YMarker((None, 1))]

    def _main(self, text):
        return [int(sent[-1] == "r") for sent in text]

@pytest.fixture
def count_vectorizer():
    return SingleFitSkTransformer.factory(
        x_marker = TextList(),
        model_cls = CountVectorizer, 
        y_marker = TextVector(()),
        name = "count_vectorizer"
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
    train_data = DataTap(iter([np.random.choice(sentences, train_size, replace=True)]), TextList()) 
    vocab = DataTap(iter([np.random.choice(sentences, train_size, replace=True)]), TextList()) 
    x_train, y_train = Split(2)(train_data)
    y_train = YTrue()(y_train)

    vectorizer = count_vectorizer()
    vectorizer.fit(vocab)
    vectorizer(x_train)
    naive_baise = naive_baise_op()

    naive_baise.fit(Merge()([vectorizer, y_train]))

    test_data = DataTap(iter([[sentences[0] for _ in range(train_size)] for i in range(2)]),
                        TextList())
    x_test = vectorizer(test_data)
    pred = naive_baise(x_test)

    for res in pred.perform():
        res.should.be.a(dict)
        print(type(res[naive_baise.markers[0]]))
        res.should.have.key(naive_baise.markers[0])
        for res_ind in res[naive_baise.markers[0]]:
            res_ind.should.equal(1)


def test_sklearn_persistance(count_vectorizer, naive_baise_op):
    sentences = [
        "That there’s some good in this world, Mr. Frodo… and it’s worth fighting for",
        "A day may come when the courage of men fails… but it is not THIS day"
    ]
    train_size = 32
    train_data = DataTap(iter([np.random.choice(sentences, train_size, replace=True)]), TextList()) 
    vocab = DataTap(iter([np.random.choice(sentences, train_size, replace=True)]), TextList()) 
    x_train, y_train = Split(2)(train_data)
    y_train = YTrue()(y_train)

    vectorizer = count_vectorizer()
    vectorizer.fit(vocab)
    vectorizer(x_train)
    naive_baise = naive_baise_op()

    naive_baise.fit(Merge()([vectorizer, y_train]))
    saver = FileSaver()
    naive_baise.save(saver)

    second_naive_baise = naive_baise_op.load(saver)

    test_data = DataTap(iter([[sentences[0] for _ in range(train_size)] for i in range(2)]),
                        TextList())
    x_test = vectorizer(test_data)
    pred = second_naive_baise(x_test)

    for res in pred.perform():
        res.should.be.a(dict)
        print(type(res[naive_baise.markers[0]]))
        res.should.have.key(naive_baise.markers[0])
        for res_ind in res[naive_baise.markers[0]]:
            res_ind.should.equal(1)




# def test_sklearn_persisted_major_deprecation():
#     pass


# def test_sklearn_persisted_minor_non_deprecation():
#     # TODO implement
#     pass