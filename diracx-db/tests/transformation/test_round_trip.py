"""Stored processes survive a round trip through the database.

MySQL's JSON type re-sorts keys and reformats documents, so a stored row is
not the bytes that were hashed; what must hold is that the value read back
re-canonicalises to the identity it is stored under. Run against real servers
as described in ``conftest.py``.
"""

from __future__ import annotations

from diracx.core.cwl import decompose, format_reference, pack, unpack
from diracx.db.sql import TransformationDB


def tool(default) -> dict:
    return {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "$namespaces": {"edam": "http://edamontology.org/"},
        "baseCommand": ["lb-prod-run"],
        "requirements": {
            "SchemaDefRequirement": {
                "types": [
                    {"name": "Seed", "type": "record", "fields": {"value": "int"}}
                ]
            },
            "EnvVarRequirement": {"envDef": {"ZETA": "1", "ALPHA": "2"}},
        },
        "inputs": {
            "seed": "Seed?",
            "data": {"type": "File[]", "format": "edam:format_2572"},
            "conf": {"type": "Any", "default": default},
        },
        "outputs": {"out": {"type": "File[]", "outputBinding": {"glob": "*.sim"}}},
    }


def workflow(*runs) -> dict:
    return {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "requirements": {"SubworkflowFeatureRequirement": {}},
        "inputs": {"data": "File[]"},
        "outputs": {},
        "steps": {
            f"s{i}": {"run": run, "in": {"data": "data"}, "out": ["out"]}
            for i, run in enumerate(runs)
        },
    }


# Values MySQL is known to normalise: key order, numbers, unicode
AWKWARD = [0.1, 1e-7, 2**53, -0.0, 1.5e300, "ünïcødé", {"z": 1, "a": 2}]


async def test_rows_read_back_re_hash_to_their_identity(
    transformation_db: TransformationDB,
):
    shared = tool("plain")
    trees = [decompose(p) for p in (shared, tool(AWKWARD))]
    shared_id = trees[0][0]
    trees.append(decompose(workflow(format_reference(shared_id), tool(AWKWARD))))
    nodes = {k: v for _, tree in trees for k, v in tree.items()}
    async with transformation_db as db:
        await db.insert_processes({root: pack(root, nodes) for root, _ in trees})
    async with transformation_db as db:
        stored = await db.get_processes([root for root, _ in trees])
    for root, _ in trees:
        assert unpack(stored[root])[0] == root
