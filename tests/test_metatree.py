import asyncio
import pickle
import pytest
import shutil
import uuid

from functools import partial
from pathlib import Path

from metatree import Metatree
from metatree.io_handler import LocalYamlHandler


@pytest.fixture(scope="session")
def shared_fixture():
    basepath = f"/tmp/{uuid.uuid4().hex[:8]}"
    Path(basepath).mkdir()
    with open(Path(f"{basepath}/trained.pkl"), "wb") as file:
        pickle.dump(
            (
                "spam",
                "eggs",
            ),
            file,
        )
    metatree = Metatree(
        f"{basepath}/metatree",
        (
            "model",
            "version",
            "stage",
        ),
        locking_enabled=True,
    )
    yield metatree, basepath
    shutil.rmtree(basepath)


def test_put(shared_fixture):
    metatree, basepath = shared_fixture
    res = metatree.put(
        {"model": "model_a", "version": "v1", "stage": "training"},
        f"{basepath}/trained.pkl",
    )
    assert res == True


def test_find(shared_fixture):
    metatree, basepath = shared_fixture
    got = metatree.find({"model": "model_a", "version": "v1", "stage": "training"})
    assert got.location == f"file://{basepath}/metatree/model_a/v1/training"
    got = metatree.find({"model": "model_a", "version": "v1"})
    assert got.location == f"file://{basepath}/metatree/model_a/v1"


def test_get(shared_fixture):
    metatree, basepath = shared_fixture
    for chunk in metatree.get("model_a/v1/training/trained.pkl"):
        with open(f"{basepath}/downloaded.pkl", "ab") as f:
            f.write(chunk)
    assert Path(f"{basepath}/downloaded.pkl").exists() == True


def test_list(shared_fixture):
    metatree, _ = shared_fixture
    got = metatree.find({"model": "model_a", "version": "v1", "stage": "training"})
    assert got.list() == ["trained.pkl"]


def test_update(shared_fixture):
    metatree, _ = shared_fixture
    got = metatree.find({"model": "model_a"})
    got.update(active="v1")
    assert got.metadata.get("active") == "v1"


def test_dict_query(shared_fixture):
    metatree, basepath = shared_fixture
    got = metatree.find(
        {
            "model": "model_a",
            "version": {"metadata": "active"},
            "stage": {"value": "training"},
        }
    )
    assert got.location == f"file://{basepath}/metatree/model_a/v1/training"


def test_string_query(shared_fixture):
    metatree, basepath = shared_fixture
    got = metatree.find("model_a/<active>/training")
    assert got.location == f"file://{basepath}/metatree/model_a/v1/training"


def test_download(shared_fixture):
    metatree, basepath = shared_fixture
    with open(f"{basepath}/empty.txt", "w") as _:
        metatree.put(
            {"model": "model_a", "version": "v1", "stage": "training"},
            f"{basepath}/empty.txt",
        )
    metatree.get("model_a/v1/training/empty.txt", outfile=f"{basepath}/downloaded.txt")
    assert Path(f"{basepath}/downloaded.txt").exists()
    recipe = Path(f"{basepath}/nested/spam/eggs/recipe")
    recipe.parent.mkdir(parents=True)
    recipe.touch()
    metatree.put("model_a/v1/training", f"{basepath}/nested/spam", recursive=True)
    metatree.get("model_a/v1/training/spam", outfile=f"{basepath}/spam", recursive=True)
    assert Path(f"{basepath}/spam/eggs/recipe").exists()


async def async_update(mtree, **kwargs):
    loop = asyncio.get_event_loop()
    func = partial(mtree.update, **kwargs)
    await loop.run_in_executor(None, func)


async def remove_lock_file(basepath):
    await asyncio.sleep(5)
    Path(f"{basepath}/.lock").unlink()


async def _test_lock(shared_fixture):
    metatree, basepath = shared_fixture
    Path(f"{basepath}/.lock").touch()
    await asyncio.gather(
        async_update(metatree, spam="eggs"),
        remove_lock_file(basepath),
    )
    # exception_occurred = 0
    # for _ in range(3):
    #     with pytest.raises(Exception):
    #         await asyncio.gather(
    #             async_update(metatree, spam="eggs"),
    #             remove_lock_file(basepath),
    #         )
    #         exception_occurred += 1
    # assert exception_occurred == 3


def test_custom_io_handler():
    basepath = f"/tmp/{uuid.uuid4().hex[:8]}"
    Path(basepath).mkdir()
    with open(Path(f"{basepath}/trained.pkl"), "wb") as file:
        pickle.dump(
            (
                "spam",
                "eggs",
            ),
            file,
        )
    metatree = Metatree(
        f"{basepath}/metatree",
        (
            "model",
            "version",
            "stage",
        ),
        locking_enabled=True,
        io_handler=LocalYamlHandler,
    )
    metatree.put(f"my-awful-model/v1/training", f"{basepath}/trained.pkl")
    shutil.rmtree(basepath)


def test_lock(shared_fixture, caplog):
    # logging.getLogger().setLevel(logging.DEBUG)
    # caplog.set_level(logging.WARNING)
    asyncio.run(_test_lock(shared_fixture))
    metatree, _ = shared_fixture
    assert metatree.metadata.get("spam") == "eggs"
