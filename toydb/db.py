import pathlib
from enum import IntEnum


class ToyDBException(Exception):
    pass


class ToyDBType(IntEnum):
    KEY = 0
    VALUE = 1
    TOMBSTONE = 2


class ToyDB:
    def __init__(self, path: pathlib.Path):
        self.encoding = "utf-8"
        self.path = path
        if not self.path.exists():
            self.path.mkdir(parents=True)
        self.file = self.path / "file.db"
        self.file.touch()

    async def get(self, key: str) -> str | None:
        with open(self.file, "br") as file:
            file_content = file.read()
        if not file_content:
            return None
        key_to_search = key.encode(self.encoding)
        value = None
        current_key = None
        index = 0
        while True:
            type_ = file_content[index]
            index += 1
            length = file_content[index]
            index += 1
            match type_:
                case ToyDBType.KEY:
                    if current_key:
                        raise ToyDBException(
                            f"Corrupt DB, type '{type_}' after type '{key_to_search}'."
                        )
                    current_key = file_content[index : index + length]
                    index += length
                case ToyDBType.VALUE:
                    if not current_key:
                        raise ToyDBException(
                            f"Corrupt DB, type '{type_}' without prior type '{0}'."
                        )
                    current_value = file_content[index : index + length]
                    index += length
                    if key_to_search == current_key:
                        value = current_value
                    # Reset current key so we can read the next value
                    current_key = None
                case ToyDBType.TOMBSTONE:
                    tombstone_key = file_content[index : index + length]
                    index += length
                    if tombstone_key == key_to_search:
                        value = None
                case other:
                    raise ToyDBException(f"Corrupt DB, unknown type '{other}'.")
            # End of file
            if index == len(file_content):
                break
        return value.decode(self.encoding) if value else None

    async def set(self, key: str, value: str) -> None:
        to_write = bytes()
        to_write += bytes([ToyDBType.KEY])
        serialized_key = key.encode(self.encoding)
        try:
            to_write += bytes([len(serialized_key)])
        except ValueError:
            raise ToyDBException(f"Key '{key}' is longer than the allowed 255 bytes.")
        to_write += serialized_key
        to_write += bytes([ToyDBType.VALUE])
        serialized_value = value.encode(self.encoding)
        try:
            to_write += bytes([len(serialized_value)])
        except ValueError:
            raise ToyDBException(
                f"Value '{value}' is longer than the allowed 255 bytes."
            )
        to_write += serialized_value
        with open(self.file, "ba") as file:
            file.write(to_write)

    async def delete(self, key: str) -> None:
        to_write = bytes()
        to_write += bytes([ToyDBType.TOMBSTONE])
        serialized_key = key.encode(self.encoding)
        try:
            to_write += bytes([len(serialized_key)])
        except ValueError:
            raise ToyDBException(
                f"Key '{key}' is longer than the allowed 255 bytes."
            )
        to_write += serialized_key
        with open(self.file, "ba") as file:
            file.write(to_write)
