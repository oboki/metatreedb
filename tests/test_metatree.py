import pytest
import uuid
import shutil
import pickle

from pathlib import Path
from metatree import Metatree


@pytest.fixture(scope="session")
def shared_fixture():
    basepath = f"/tmp/{uuid.uuid4().hex[:8]}"
    Path(basepath).mkdir()
    with open(Path(f"{basepath}/trained.pkl"), "wb") as file:
        pickle.dump(("spam","eggs",), file)
    metatree = Metatree(
        f"{basepath}/metatree",
        (
            "model",
            "version",
            "stage",
        ),
    )
    metatree.init()
    yield metatree, basepath
    shutil.rmtree(basepath)


def test_put(shared_fixture):
    metatree, basepath = shared_fixture
    res = metatree.put(
        {"model": "model_a", "version": "v1", "stage": "training"},
        f"{basepath}/trained.pkl",
    )
    assert res == True


def test_search(shared_fixture):
    metatree, basepath = shared_fixture
    got = metatree.search({"model": "model_a", "version": "v1", "stage": "training"})
    assert got.location == f"{basepath}/metatree/model_a/v1/training"
    got = metatree.search({"model": "model_a", "version": "v1"})
    assert got.location == f"{basepath}/metatree/model_a/v1"


def test_get(shared_fixture):
    metatree, basepath = shared_fixture
    for chunk in metatree.get("model_a/v1/training/trained.pkl"):
        with open(f"{basepath}/downloaded.pkl", "ab") as f:
            f.write(chunk)
    assert Path(f"{basepath}/downloaded.pkl").exists() == True


def test_list(shared_fixture):
    metatree, _ = shared_fixture
    got = metatree.search({"model": "model_a", "version": "v1", "stage": "training"})
    assert got.list() == ["trained.pkl"]


def test_update(shared_fixture):
    metatree, _ = shared_fixture
    got = metatree.search({"model": "model_a"})
    got.update(active="v1")
    assert got.metadata.get("active") == "v1"


def test_query(shared_fixture):
    metatree, basepath = shared_fixture
    got = metatree.search(
        {
            "model": "model_a",
            "version": {"metadata": "active"},
            "stage": {"value": "training"},
        }
    )
    assert got.location == f"{basepath}/metatree/model_a/v1/training"


def test_query_str(shared_fixture):
    metatree, basepath = shared_fixture
    got = metatree.search("model_a/<active>/training")
    assert got.location == f"{basepath}/metatree/model_a/v1/training"
