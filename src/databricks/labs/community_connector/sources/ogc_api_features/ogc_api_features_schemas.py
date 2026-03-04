"""Schemas, helpers, and constants for the OGC API Features connector."""

import json

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

RETRIABLE_STATUS_CODES = {429, 500, 503}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0

# JSON Schema type → Spark SQL type.  "integer" maps to LongType (not IntegerType).
JSON_SCHEMA_TYPE_MAP = {
    "string": StringType(),
    "number": DoubleType(),
    "integer": LongType(),
    "boolean": BooleanType(),
}

# Python type → Spark SQL type (used when inferring from feature values).
PYTHON_TYPE_MAP = {
    str: StringType(),
    int: LongType(),
    float: DoubleType(),
    bool: BooleanType(),
}


def _spark_type_for_json_schema(prop: dict):
    """Return a Spark DataType for a JSON Schema property descriptor."""
    json_type = prop.get("type", "string")
    if isinstance(json_type, list):
        # e.g. ["string", "null"] — pick the first non-null type
        json_type = next((t for t in json_type if t != "null"), "string")
    return JSON_SCHEMA_TYPE_MAP.get(json_type, StringType())


def schema_from_json_schema(json_schema: dict) -> StructType:
    """Build a StructType from an OGC JSON Schema response.

    Always prepends ``id`` (StringType, non-nullable) and ``geometry``
    (StringType, nullable) columns before the properties.
    """
    fields = [
        StructField("id", StringType(), nullable=False),
        StructField("geometry", StringType(), nullable=True),
    ]
    properties = json_schema.get("properties", {})
    for name, prop in properties.items():
        if name in ("id", "geometry"):
            continue
        fields.append(StructField(name, _spark_type_for_json_schema(prop), nullable=True))
    return StructType(fields)


def schema_from_features(features: list[dict]) -> StructType:
    """Infer a StructType from a list of GeoJSON feature dicts.

    Scans all features to collect the union of property keys and infer
    types from the first non-None value encountered for each key.
    """
    fields = [
        StructField("id", StringType(), nullable=False),
        StructField("geometry", StringType(), nullable=True),
    ]
    prop_types: dict[str, type] = {}
    prop_order: list[str] = []
    for feature in features:
        for key, value in (feature.get("properties") or {}).items():
            if key in ("id", "geometry"):
                continue
            if key not in prop_types:
                prop_order.append(key)
                prop_types[key] = type(None)
            if value is not None and prop_types[key] is type(None):
                prop_types[key] = type(value)

    for key in prop_order:
        spark_type = PYTHON_TYPE_MAP.get(prop_types[key], StringType())
        fields.append(StructField(key, spark_type, nullable=True))

    return StructType(fields)


def flatten_feature(feature: dict) -> dict:
    """Flatten a GeoJSON Feature into a flat dict for ingestion.

    - Lifts ``properties`` to top level
    - Serializes ``geometry`` as JSON string
    - Coerces ``id`` to string
    """
    record = {}
    record["id"] = str(feature.get("id", ""))
    geom = feature.get("geometry")
    record["geometry"] = json.dumps(geom) if geom is not None else None
    for key, value in (feature.get("properties") or {}).items():
        if key in ("id", "geometry"):
            continue
        record[key] = value
    return record
