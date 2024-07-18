from metatree.metatree import MetaTree

def test_location():
    metatree = MetaTree("/tmp/metatree", ["model", "version", "stage"])
    assert metatree.location == "/tmp/metatree/"
    metatree = metatree.search({"model": "model1", "version": "v1", "stage": "dev"})
    assert metatree.location == "/tmp/metatree/model1/v1/dev"