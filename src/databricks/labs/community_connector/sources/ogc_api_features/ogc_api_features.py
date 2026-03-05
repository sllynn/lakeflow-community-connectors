"""OGC API Features connector for Lakeflow Connect."""

import time
from typing import Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.ogc_api_features.ogc_api_features_schemas import (
    INITIAL_BACKOFF,
    MAX_RETRIES,
    RETRIABLE_STATUS_CODES,
    flatten_feature,
    schema_from_features,
    schema_from_json_schema,
)


class OgcApiFeaturesLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for OGC API Features endpoints."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._base_url = options["base_url"].rstrip("/")
        self._api_key = options.get("api_key")
        self._api_key_header = options.get("api_key_header", "X-API-Key")
        self._session = requests.Session()
        if self._api_key:
            self._session.headers[self._api_key_header] = self._api_key

    def _request_with_retry(self, url: str, params: dict | None = None) -> requests.Response:
        """Issue a GET request with exponential backoff on retriable status codes."""
        backoff = INITIAL_BACKOFF
        resp = None
        for attempt in range(MAX_RETRIES):
            resp = self._session.get(url, params=params)

            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            if attempt < MAX_RETRIES - 1:
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else backoff
                time.sleep(wait)
                backoff *= 2

        return resp

    def list_tables(self) -> list[str]:
        resp = self._request_with_retry(f"{self._base_url}/collections", params={"f": "json"})
        resp.raise_for_status()
        data = resp.json()
        return [
            c["id"]
            for c in data.get("collections", [])
            if c.get("itemType") == "feature"
        ]

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        # Try the JSON Schema endpoint first
        resp = self._request_with_retry(
            f"{self._base_url}/collections/{table_name}/schema",
            params={"f": "json"},
        )
        if resp.status_code == 200:
            try:
                return schema_from_json_schema(resp.json())
            except Exception:
                pass

        # Fall back to inferring from first page of features
        params = {"f": "json", "limit": table_options.get("limit", "10")}
        resp = self._request_with_retry(
            f"{self._base_url}/collections/{table_name}/items",
            params=params,
        )
        resp.raise_for_status()
        features = resp.json().get("features", [])
        if not features:
            raise RuntimeError(
                f"Cannot infer schema for '{table_name}': no features returned"
            )
        return schema_from_features(features)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        return {
            "primary_keys": ["id"],
            "ingestion_type": "snapshot",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # Snapshot: if we already read (start_offset is truthy), signal done
        if start_offset:
            return iter([]), start_offset

        records = []
        params: dict[str, str] = {"f": "json"}

        # "limit" caps the total number of records fetched and is also passed
        # as the OGC page-size hint.  When omitted, all features are retrieved.
        max_records = int(table_options["limit"]) if "limit" in table_options else None

        # Pass through supported OGC query parameters from table_options
        for key in ("limit", "bbox", "datetime", "crs"):
            if key in table_options:
                params[key] = table_options[key]

        url = f"{self._base_url}/collections/{table_name}/items"

        while url:
            resp = self._request_with_retry(url, params=params)
            resp.raise_for_status()
            body = resp.json()

            for feature in body.get("features", []):
                records.append(flatten_feature(feature))
                if max_records is not None and len(records) >= max_records:
                    return iter(records), {}

            # Follow HATEOAS next link
            url = None
            for link in body.get("links", []):
                if link.get("rel") == "next":
                    url = link["href"]
                    # After the first request, params are encoded in the next URL
                    params = {}
                    break

        return iter(records), {}
