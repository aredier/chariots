"""module that tests the sci-kit learn MLOp API"""
from chariots.runners import SequentialRunner


def test_sk_training_pipeline(basic_sk_pipelines):  # pylint: disable=invalid-name
    """function that trains that sci-kit learn based op and checks laod and reload"""
    train_pipe, pred_pipe = basic_sk_pipelines

    runner = SequentialRunner()
    runner.run(train_pipe)
    op_bytes = train_pipe.node_for_name['sklrop']._op.serialize()  # pylint: disable=protected-access
    pred_pipe.node_for_name['sklrop']._op.load(op_bytes)  # pylint: disable=protected-access
    response = runner.run(pred_pipe, pipeline_input=[[100], [101], [102]])

    for i, individual_value in enumerate(response):
        assert abs(101 + i - individual_value) < 1e-5
