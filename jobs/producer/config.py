from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel


class StreamConfig(BaseModel):
    type: str
    symbols: list[str]


class KafkaConfig(BaseModel):
    raw_topic: str


class ProducerRuntimeConfig(BaseModel):
    reconnect_delay_seconds: int
    max_reconnect_delay_seconds: int


class ProducerConfig(BaseModel):
    exchange: str
    stream: StreamConfig
    kafka: KafkaConfig
    producer: ProducerRuntimeConfig


def load_config(config_path: str | Path) -> dict[str, Any]:
    with Path(config_path).open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def load_producer_config(config_path: str | Path) -> ProducerConfig:
    return ProducerConfig.model_validate(load_config(config_path))
