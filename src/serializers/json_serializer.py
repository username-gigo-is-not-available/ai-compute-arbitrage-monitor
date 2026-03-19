from typing import Any

from pydantic import BaseModel


class JsonSerializer:

    @classmethod
    def serialize(cls, data: BaseModel) -> dict[str, Any]:
        return data.model_dump(mode="json")

    @classmethod
    def serialize_batch(cls, data: list[BaseModel]) -> list[dict[str, Any]]:
        return [JsonSerializer.serialize(entry) for entry in data]
