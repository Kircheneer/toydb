import asyncio
import os
from functools import partial

import pytest

from toydb.db import ToyDB


@pytest.fixture
def db(tmp_path):
    return ToyDB(tmp_path)


@pytest.mark.asyncio
async def test_db_set_and_get(db):
    assert await db.get("key") is None
    await db.set("key", "value")
    assert await db.get("key") == "value"


@pytest.mark.asyncio
async def test_db_duplicate_add(db):
    await db.set("key", "value")
    await db.set("key", "updated value")
    assert await db.get("key") == "updated value"


@pytest.mark.asyncio
async def test_db_delete(db):
    await db.set("key", "value")
    await db.delete("key")
    assert await db.get("key") is None


@pytest.mark.asyncio
async def test_multiple_data_files(db):
    await db.set("first_key", "first_value")
    # Fill the DB enough that it has to create a second data file
    for i in range(100):
        await db.set(str(i), str(i * 2))
    await db.set("last_key", "last_value")
    # Assert that there are now at least 2 data files
    assert len(list(db.files)) > 1
    # Assert that the first and last values written are both readable
    assert await db.get("first_key") == "first_value"
    assert await db.get("last_key") == "last_value"


@pytest.mark.asyncio
async def test_compact(db):
    await db.set("deleted", "")
    await db.set("present", "value")
    await db.delete("deleted")
    size_before_compact = os.path.getsize(db.file)
    await db.compact()
    size_after_compact = os.path.getsize(db.file)
    assert size_before_compact > size_after_compact
    assert await db.get("present") == "value"
    assert await db.get("deleted") is None


@pytest.mark.skip
def test_performance(db, benchmark):
    """WIP attempt at some performance testing."""
    for i in range(1000):
        asyncio.run(db.set(str(i), str(i)))

    def to_benchmark():
        asyncio.run(db.get(key="0"))

    benchmark(to_benchmark)
