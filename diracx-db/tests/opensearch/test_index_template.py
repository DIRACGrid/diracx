from __future__ import annotations

from datetime import datetime, timezone

import opensearchpy
import pytest

from diracx.testing.osdb import DummyOSDB

DUMMY_DOCUMENT = {
    "DateField": datetime.now(tz=timezone.utc),
    "IntField": 1234,
    "KeywordField1": "keyword1",
    "KeywordField2": "keyword two",
    "TextField": "text value",
}


async def test_applies_new_indices(dummy_opensearch_db: DummyOSDB):
    """Ensure that the index template is applied to new indices."""
    index_mappings = await _get_test_index_mappings(dummy_opensearch_db)
    # Ensure the index template was applied during index creation
    assert index_mappings == {"properties": dummy_opensearch_db.fields}


async def dummy_opensearch_db_without_template(dummy_opensearch_db: DummyOSDB):
    """Sanity test that previous test fails if there isn't a template."""
    index_mappings = await _get_test_index_mappings(dummy_opensearch_db)
    # Ensure the mappings are different to the expected ones
    assert index_mappings != {"properties": dummy_opensearch_db.fields}


async def _get_test_index_mappings(dummy_opensearch_db: DummyOSDB):
    vo = "dummyvo"
    document_id = 1
    index_name = dummy_opensearch_db.index_name(vo, document_id)

    # At this point the index should not exist yet
    with pytest.raises(opensearchpy.exceptions.NotFoundError):
        await dummy_opensearch_db.client.indices.get_mapping(index_name)

    # Insert document which will automatically create the index based on the template
    await dummy_opensearch_db.upsert(vo, document_id, DUMMY_DOCUMENT)

    # Ensure the result looks as expected and return the mappings
    index_mapping = await dummy_opensearch_db.client.indices.get_mapping(index_name)
    assert list(index_mapping) == [index_name]
    assert list(index_mapping[index_name]) == ["mappings"]
    return index_mapping[index_name]["mappings"]
