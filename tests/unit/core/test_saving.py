import hashlib
import json
import os
import random

import dill

from chariots.core import saving


def test_file_saver(tmpdir):
    saver = saving.FileSaver(tmpdir)
    fake_string = hashlib.sha1(str(random.randint(0, 1000)).encode("utf-8")).hexdigest()
    saver.save(fake_string.encode("utf-8"), "/foo/bar.bin")

    file_path = os.path.join(tmpdir, "foo/bar.bin")
    assert os.path.exists(file_path)
    with open(file_path, "rb") as file:
        assert file.read().decode("utf-8") == fake_string

    loaded_bytes = saver.load("foo/bar.bin")
    assert loaded_bytes.decode("utf-8") == fake_string


def test_dill_serialisation():
    dill_serializer = saving.DillSerializer()

    def fake_function():
        return 42

    serialized_bytes = dill_serializer.serialize_object(fake_function)
    del fake_function

    hand_deserialized = dill.loads(serialized_bytes)
    assert hand_deserialized() == 42
    del hand_deserialized

    deserialized = dill_serializer.deserialize_object(serialized_bytes)
    assert deserialized() == 42


def test_json_serialisation():
    json_serializer = saving.JSONSerializer()

    mapping = {"foo": 42}
    serialised_bytes = json_serializer.serialize_object(mapping)

    hand_deserialized = json.loads(serialised_bytes.decode("utf-8"))
    assert hand_deserialized == mapping

    deserialized = json_serializer.deserialize_object(serialised_bytes)
    assert deserialized == mapping