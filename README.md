# metatreedb

__Metatree__ is a DBMS that uses the filesystem itself to organize and manage data in a tree-structured format.

In `metadata.json` in each tree node, you can manage information about child nodes, which can also be used for searching.

## Features

* metadata-based index
* db-level concurrency control

## Installation

```bash
pip install metatreedb
```

## Quick Start

Here's an example of using Metatree as a model repository by setting up a database with `(model_name, version,)` as identifiers:

```bash
from metatree import Metatree

metatree = Metatree(
    "/tmp/my-model-repository",
    (
        "model",
        "version",
    ),
)

import uuid
import pickle

from pathlib import Path

for i in range(1, 4):
    awful_uuid = uuid.uuid4()
    trained = Path("/tmp/metatree-files/trained.pkl")
    with open(trained, "wb") as f:
        pickle.dump(awful_uuid, f)
    metatree.put(f"my-awful-model/v{i}", trained)
```

This will create files and directories in your filesystem as shown:

```bash
❯ tree /tmp/my-model-repository
/tmp/my-model-repository
├── metadata.json
└── my-awful-model
    ├── metadata.json
    ├── v1
    │   ├── metadata.json
    │   └── trained.pkl
    ├── v2
    │   ├── metadata.json
    │   └── trained.pkl
    └── v3
        ├── metadata.json
        └── trained.pkl
```


To add metadata information, use `find` and `update`:

```python
metatree.find("my-awful-model")
metatree.update(active="v2")
for i in range(1, 4):
    metatree.find(f"my-awful-model/v{i}").update(model_file="trained.pkl")
```

This will update the `metadata.json` files as follows:

```bash
❯ cat /tmp/my-model-repository/my-awful-model/metadata.json
{"children": ["v1", "v2", "v3"], "active": "v2"}

❯ cat /tmp/my-model-repository/my-awful-model/v*/metadata.json
{"model_file": "trained.pkl"}
{"model_file": "trained.pkl"}
{"model_file": "trained.pkl"}
```

You can use this search index to find files. By enclosing the keys from `metadata.json` within angle brackets `<>` and substituting them in location, you can perform searches as follows:

```python
metatree.find("my-awful-model/<active>")
print(metatree.location)
# This returns `file:///tmp/my-model-repository/my-awful-model/v2

file = metatree.get("my-awful-model/<active>/<model_file>")
print(file)
# The given path translates to `my-awful-model/v2/trained.pkl`,
# and it returns generator object
```
