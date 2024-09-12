from fastapi import FastAPI, Depends
from pydantic import BaseModel

from toydb import ToyDB
from toydb._utils import dirs

app = FastAPI()


async def get_db() -> ToyDB:
    return ToyDB(dirs.user_data_dir)


class GetValueResponse(BaseModel):
    value: str | None


class SetValueRequest(BaseModel):
    value: str


class SetValueResponse(BaseModel):
    pass


class DeleteValueResponse(BaseModel):
    pass


class DropDatabaseResponse(BaseModel):
    pass


@app.get("/v1/db/{key}")
async def get_value(key: str, db: ToyDB = Depends(get_db)) -> GetValueResponse:
    value = await db.get(key)
    return GetValueResponse(value=value)


@app.post("/v1/db/{key}")
async def set_value(key: str, value: SetValueRequest, db: ToyDB = Depends(get_db)) -> SetValueResponse:
    await db.set(key, value.value)
    return SetValueResponse()


@app.delete("/v1/db/{key}")
async def delete_key(key: str, db: ToyDB = Depends(get_db)) -> DeleteValueResponse:
    await db.delete(key)
    return DeleteValueResponse()


@app.delete("/v1/db")
async def drop_database(db: ToyDB = Depends(get_db)) -> DropDatabaseResponse:
    await db.drop()
    return DropDatabaseResponse()