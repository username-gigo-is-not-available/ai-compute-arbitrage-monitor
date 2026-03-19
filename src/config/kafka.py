import os

from pydantic import BaseModel, Field


class KafkaConfig(BaseModel):
    bootstrap_servers: str = Field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    api_key: str = Field(default_factory=lambda: os.getenv("KAFKA_API_KEY", ""))
    api_secret: str = Field(default_factory=lambda: os.getenv("KAFKA_API_SECRET", ""))
