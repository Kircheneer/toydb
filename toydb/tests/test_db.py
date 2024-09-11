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
