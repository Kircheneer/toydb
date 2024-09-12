import asyncio
import dataclasses
import os.path
import pathlib
from builtins import str
from enum import IntEnum
from typing import Generator, Self

import aiofiles


class ToyDBException(Exception):
    """Exception raised when any operations in ToyDB go wrong."""

    pass


class ToyDBType(IntEnum):
    """Types for the type-length-value encoding of ToyDB."""

    KEY = 0
    VALUE = 1
    TOMBSTONE = 2


@dataclasses.dataclass
class ToyDBRecord:
    """Individual database record."""

    key: bytes
    value: bytes | None
    tombstone: bool

    __slots__ = (
        "key",
        "value",
        "tombstone",
    )

    def serialize(self) -> bytes:
        """Serialize this record to bytes."""
        if not self.tombstone:
            to_write = bytes()
            to_write += bytes([ToyDBType.KEY])
            serialized_key = self.key
            try:
                to_write += bytes([len(serialized_key)])
            except ValueError:
                raise ToyDBException(
                    f"Key '{self.key}' is longer than the allowed 255 bytes."
                )
            to_write += serialized_key
            to_write += bytes([ToyDBType.VALUE])
            serialized_value = self.value
            try:
                to_write += bytes([len(serialized_value)])
            except ValueError:
                raise ToyDBException(
                    f"Value '{self.value}' is longer than the allowed 255 bytes."
                )
            to_write += serialized_value
            return to_write
        else:
            to_write = bytes()
            to_write += bytes([ToyDBType.TOMBSTONE])
            serialized_key = self.key
            try:
                to_write += bytes([len(serialized_key)])
            except ValueError:
                raise ToyDBException(
                    f"Key '{self.key}' is longer than the allowed 255 bytes."
                )
            to_write += serialized_key
            return to_write

    @classmethod
    async def deserialize(cls, file) -> Self | None:
        key = None
        while type_ := await file.read(1):
            length = int.from_bytes(await file.read(1), byteorder="big")
            match int.from_bytes(type_, byteorder="big"):
                case ToyDBType.KEY:
                    if key:
                        raise ToyDBException(
                            f"Corrupt DB, type '{ToyDBType.KEY}' after type '{key}'."
                        )
                    key = await file.read(length)
                case ToyDBType.VALUE:
                    if not key:
                        raise ToyDBException(
                            f"Corrupt DB, type '{ToyDBType.VALUE}' without prior type '{ToyDBType.KEY}'."
                        )
                    return ToyDBRecord(
                        key=key,
                        value=await file.read(length),
                        tombstone=False,
                    )
                case ToyDBType.TOMBSTONE:
                    return ToyDBRecord(
                        key=await file.read(length), value=None, tombstone=True
                    )
                case other:
                    raise ToyDBException(f"Corrupt DB, unknown type '{other}'.")
        return None


class ToyDB:
    def __init__(self, path: pathlib.Path | str):
        """Initialize ToyDB instance.

        :param path: The path (i.e. directory) under which to save the data files."""
        self.path = path if isinstance(path, pathlib.Path) else pathlib.Path(path)
        if not self.path.exists():
            self.path.mkdir(parents=True)
        elif not self.path.is_dir():
            raise ToyDBException(f"Path '{self.path}' is not a directory.")

        self.encoding = "utf-8"
        # Artificially low to mage things pertaining to multiple files easier to test
        self.max_file_size = 255

        self.data_file_index = 0
        while True:
            if not self.file.exists():
                if self.data_file_index == 0:
                    self.file.touch()
                else:
                    self.data_file_index -= 1
                break
            self.data_file_index += 1

    @property
    def file(self) -> pathlib.Path:
        """Get the path to the current data file."""
        return self._get_data_file(self.data_file_index)

    @property
    def files(self) -> Generator[pathlib.Path, None, None]:
        """Get all data files."""
        return (self._get_data_file(i) for i in range(self.data_file_index + 1))

    def _get_data_file(self, index: int) -> pathlib.Path:
        return self.path / f"data{index}.db"

    def _get_temp_data_file(self, index: int) -> pathlib.Path:
        return self.path / f"tempdata{index}.db"

    async def drop(self) -> None:
        """Drops the entire database."""
        for file in self.files:
            file.unlink(missing_ok=True)

    async def iterate(
        self, index: int | None = None
    ) -> Generator[ToyDBRecord, None, None]:
        files = self.files if not index else [list(self.files)[index]]
        for path in files:
            async with aiofiles.open(path, "br") as file:
                while record := await ToyDBRecord.deserialize(file):
                    yield record

    async def merge(self):
        """Merge file segments.

        TODO: This currently doesn't merge anything but just writes all the files again. Whoops!"""
        await asyncio.gather(
            *[self.compact(index=i) for i in range(self.data_file_index)]
        )
        buffer = bytes()
        index = 0
        async for record in self.iterate():
            serialized_record = record.serialize()
            if len(buffer) + len(serialized_record) <= self.max_file_size:
                buffer += serialized_record
            else:
                async with aiofiles.open(
                    self._get_temp_data_file(index=index), "ba"
                ) as file:
                    await file.write(buffer)
                buffer = serialized_record
                index += 1
        await self.drop()
        for i in range(index):
            self._get_temp_data_file(index=i).rename(self._get_data_file(index=i))
        self.data_file_index = index

    async def compact(self, index: int | None = None) -> None:
        """Compact the data file at the index, defaults to the current data file."""
        if not index:
            index = self.data_file_index
        values: dict[bytes, bytes] = {}
        tombstones: set[bytes] = set()
        current_key = None
        file_to_compact = self._get_data_file(index)
        async with aiofiles.open(file_to_compact, "br") as file:
            while type_ := await file.read(1):
                length = int.from_bytes(await file.read(1), byteorder="big")
                match int.from_bytes(type_, byteorder="big"):
                    case ToyDBType.KEY:
                        if current_key:
                            raise ToyDBException(
                                f"Corrupt DB, type '{ToyDBType.KEY}' after type '{current_key}'."
                            )
                        current_key = await file.read(length)
                    case ToyDBType.VALUE:
                        if not current_key:
                            raise ToyDBException(
                                f"Corrupt DB, type '{ToyDBType.VALUE}' without prior type '{0}'."
                            )
                        values[current_key] = await file.read(length)
                        if current_key in tombstones:
                            tombstones.remove(current_key)
                        # Reset current key so we can read the next value
                        current_key = None
                    case ToyDBType.TOMBSTONE:
                        tombstone_key = await file.read(length)
                        del values[tombstone_key]
                        tombstones.add(tombstone_key)
                    case other:
                        raise ToyDBException(f"Corrupt DB, unknown type '{other}'.")
        file_to_compact.unlink()
        file_to_compact.touch()
        for key, value in values.items():
            await self.set(key.decode(self.encoding), value.decode(self.encoding))
        for key in tombstones:
            await self.delete(key.decode(self.encoding))

    async def get(self, key: str) -> str | None:
        """Get the value behind the given key or None if it isn't present."""
        value = None
        async for record in self.iterate():
            if record.key == key.encode(self.encoding):
                if record.tombstone:
                    value = None
                else:
                    value = record.value
        return value.decode(self.encoding) if value else None

    async def set(self, key: str, value: str) -> None:
        """Set the given key to the given value."""
        record = ToyDBRecord(
            key=key.encode(self.encoding),
            value=value.encode(self.encoding),
            tombstone=False,
        )
        async with aiofiles.open(self.file, "ba") as file:
            await file.write(record.serialize())
        if os.path.getsize(self.file) > self.max_file_size:
            self.data_file_index += 1

    async def delete(self, key: str) -> None:
        """Delete the given key."""
        record = ToyDBRecord(key=key.encode(self.encoding), value=None, tombstone=True)
        async with aiofiles.open(self.file, "ba") as file:
            await file.write(record.serialize())
