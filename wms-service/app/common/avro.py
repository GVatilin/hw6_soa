import json
import re
from pathlib import Path
from typing import Any, Dict

import requests
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

SCHEMA_DIR = Path("/service/schemas")
SUBJECT = "warehouse-events-value"
SCHEMA_PATTERN = re.compile(r"^warehouse_event_v(?P<version>\d+)\.avsc$")


def available_schema_versions() -> list[int]:
    versions: list[int] = []
    for path in SCHEMA_DIR.iterdir():
        match = SCHEMA_PATTERN.match(path.name)
        if match:
            versions.append(int(match.group("version")))
    if not versions:
        raise FileNotFoundError(f"No schema files matching warehouse_event_vN.avsc found in {SCHEMA_DIR}")
    return sorted(versions)


def latest_schema_version() -> int:
    return available_schema_versions()[-1]


def load_schema(version: int) -> str:
    path = SCHEMA_DIR / f"warehouse_event_v{version}.avsc"
    return path.read_text(encoding="utf-8")


def json_dict_to_obj(obj: Dict[str, Any], ctx: Any) -> Dict[str, Any]:
    return obj


def obj_to_json_dict(obj: Dict[str, Any], ctx: Any) -> Dict[str, Any]:
    return obj


def make_schema_registry_client(url: str) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url})


def wait_for_schema_registry(url: str, timeout_seconds: int = 90) -> None:
    import time

    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(f"{url}/subjects", timeout=3)
            if response.status_code < 500:
                return
        except Exception as exc:
            last_error = exc
        time.sleep(2)
    raise RuntimeError(f"Schema Registry is not ready at {url}: {last_error}")


def register_schema(url: str) -> Dict[str, int]:
    wait_for_schema_registry(url)
    requests.put(
        f"{url}/config/{SUBJECT}",
        json={"compatibility": "BACKWARD"},
        timeout=10,
    ).raise_for_status()

    client = make_schema_registry_client(url)
    registered_ids: Dict[str, int] = {}
    for version in available_schema_versions():
        schema_id = client.register_schema(SUBJECT, Schema(load_schema(version), "AVRO"))
        registered_ids[f"v{version}"] = schema_id
    return registered_ids


def make_avro_serializer(url: str, version: int) -> AvroSerializer:
    client = make_schema_registry_client(url)
    schema_str = load_schema(version)
    return AvroSerializer(
        client,
        schema_str,
        obj_to_json_dict,
        conf={"auto.register.schemas": False, "subject.name.strategy": lambda ctx, schema_name: SUBJECT},
    )


def make_avro_deserializer(url: str) -> AvroDeserializer:
    client = make_schema_registry_client(url)
    return AvroDeserializer(client, from_dict=json_dict_to_obj)


def schema_as_json(version: int) -> Dict[str, Any]:
    return json.loads(load_schema(version))
