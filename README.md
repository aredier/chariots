# Chariots

Chariots is a framework for building versioned machine learning pipelines. it aims at bringing cementic versioning to most machine learning framework (sci-kit learn, keras, tensorflow, pytorch, …).

The main idea of ths framwork is to be able to deprecate models basedf on the evolution of the piepline they are integrated into and eventually (this is very much a work in progress) monitor easilly the performance impact of various changes made to a pipeline.

## Installing

this repo is not available on pypi for now so you will have to install it manually

```
git clone https://github.com/aredier/chariots.git
cd chariots
pip install .
```

## Getting started

Chariots revolves around ops to build pipeline. 

here is an example of an Op:

```python
from chariots.core.ops import BaseOp
from chariots.core import requirements

MyCustomMarker = markers.Number.new_marker()

class MyFirstOp(BaseOp):
    name = "first_chariot_op"
    requires = {"input_data": markers.Matrix((10))}
    markers = [MyCustomMarker()]
    
    def _main(self, input_data):
        return sum(input_data)
```



here is what happened

- specify the requirements of your Op (here a Matrix for instance). this will mark what needs to be in the data for the op to work
- specify your op's marker: This will marked the data produced by this op (and be used by down stream ops' requirements 
- define your ops behavior in the `_main` method:

in order to make your op run you need perform it on data. In order to do this we link the op to a `DataTap` instance

```python
from chariots.core.taps import DataTap
my_data = [list(range(10)) for _ in range(5)]
tap = DataTap(iter(my_data), markers.Matrix((10)))
op = MyFirstOp()
result = op(tap)
```

here we have initilized the tap with an iterator and a `Marker` 

we can then check the output

```python
for individual_result in result.perform():
    print(individual_result[op.markers[0]])
```

here we can see that the results will be a dictionnary with the marker of the data outputed as keys and the results as value 



### versioning

if you want some field of your op to be versioned, you can simply add it as `VersionedField`:

```python
from chariots.core.ops import BaseOp
from chariots.core import requirements
from chariots.core import versioning

MyCustomMarker = markers.Number.new_marker()

class MyFirstVersionedOp(BaseOp):
    name = "first_chariot_op"
    requires = {"input_data": markers.Matrix((10))}
    markers = [MyCustomMarker()]
    my_versioned_field = versioning.VersionField(versioning.VersionType.MAJOR,
                                                 default="hello world")
    def main(self, input_data):
        return self.my_versioned_field
```



### trainable op

In order to build a trainable op (that represents a model), you can either inherit from `TrainableOp` in a similar way as above exemples or use a factory in one of the `training` submodules:

```python
from chariots.training import sklearn

naive_baise = sklearn.SingleFitSkTransformer.factory(
	x_marker=TextList(),
	model_cls=CountVectorizer,
	y_marker=TextVector(()),
	name="count_vectorizer"
)
```



## Running the tests

to run the tests you can simply run 

```
py.test test/
```

## Contributing

If you'd like to contribute to this repository please first discuss the change you wish to make via issue, email, or any other method with me. Don't hesitate to ask me for help if you have a hard time getting in the code base ...

### Pull request process

1. Update the README.md with details of changes to the interface.
2. make sure you have some documentation in your pull request
3. Ask me to review your pull request.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
