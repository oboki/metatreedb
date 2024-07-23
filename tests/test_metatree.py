import pytest
import uuid
import shutil

from pathlib import Path
from metatree import Metatree


@pytest.fixture(scope="session")
def shared_fixture():
    basepath = f"/tmp/{uuid.uuid4().hex[:8]}"
    Path(basepath).mkdir()
    Path(f"{basepath}/trained.pkl").touch()
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


def test_get(shared_fixture):
    metatree, basepath = shared_fixture
    got = metatree.get({"model": "model_a", "version": "v1", "stage": "training"})
    assert got.location == f"{basepath}/metatree/model_a/v1/training"
    got = metatree.get({"model": "model_a", "version": "v1"})
    assert got.location == f"{basepath}/metatree/model_a/v1"


def test_list(shared_fixture):
    metatree, _ = shared_fixture
    got = metatree.get({"model": "model_a", "version": "v1", "stage": "training"})
    assert got.list() == ["trained.pkl"]


def test_update(shared_fixture):
    metatree, _ = shared_fixture
    got = metatree.get({"model": "model_a"})
    got.update(active="v1")
    assert got.metadata.get("active") == "v1"


def test_query(shared_fixture):
    metatree, basepath = shared_fixture
    got = metatree.get(
        {
            "model": "model_a",
            "version": {"metadata": "active"},
            "stage": {"value": "training"},
        }
    )
    assert got.location == f"{basepath}/metatree/model_a/v1/training"


def test_query_str(shared_fixture):
    metatree, basepath = shared_fixture
    got = metatree.get("model_a/<active>/training")
    assert got.location == f"{basepath}/metatree/model_a/v1/training"
