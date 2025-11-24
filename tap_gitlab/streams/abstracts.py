from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple, Iterator
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_record,
    write_schema,
    metadata
)
from dateutil import parser
from datetime import datetime, timezone

LOGGER = get_logger()


class BaseStream(ABC):
    url_endpoint = ""
    path = ""
    page_size = 100
    next_page_key = ""
    headers = {}
    children = []
    parent = ""
    data_key = None
    parent_bookmark_key = ""

    def __init__(self, client=None, catalog=None) -> None:
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync = []
        self.params = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        pass

    @property
    @abstractmethod
    def replication_method(self) -> str:
        pass

    @property
    @abstractmethod
    def replication_keys(self) -> str:
        pass

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        pass

    def is_selected(self):
        return metadata.get(self.metadata, (), "selected")

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        pass

    def get_records(self) -> Iterator:
        """Interacts with API client interaction and pagination."""
        self.params["per_page"] = self.page_size
        current_page = 1
        has_more_pages = True

        while has_more_pages:
            self.params["page"] = current_page

            response = self.client.get(
                self.url_endpoint, self.params, self.headers, self.path
            )

            if isinstance(response, list):
                raw_records = response
            else:
                raw_records = response.get(self.data_key, []) if self.data_key else response

            yield from raw_records

            # Check pagination headers from client response
            # GitLab returns x-next-page header when there's a next page
            response_headers = getattr(self.client, 'last_response_headers', {})
            x_next_page = response_headers.get('x-next-page') or response_headers.get('X-Next-Page')

            if x_next_page:
                # GitLab indicates next page exists
                current_page += 1
                has_more_pages = True
            elif not raw_records or len(raw_records) < self.page_size:
                # No more pages if empty response or fewer records than page_size
                has_more_pages = False
            else:
                # Try next page, will stop if empty
                current_page += 1

    def write_schema(self) -> None:
        write_schema(self.tap_stream_id, self.schema, self.key_properties)

    def update_params(self, **kwargs) -> None:
        self.params.update(kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        return record

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        return self.url_endpoint or f"{self.client.base_url}/{self.path}"


class IncrementalStream(BaseStream):
    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        return get_bookmark(  # pylint: disable=E1121
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def update_bookmark_state(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        if not (key or self.replication_keys):
            return state

        bookmark_key = key or self.replication_keys[0]
        current_bookmark = get_bookmark(  # pylint: disable=E1121
            state, stream, bookmark_key, self.client.config["start_date"]
        )
        try:
            value = max(current_bookmark, value)
        except Exception:
            LOGGER.warning("Failed to compare bookmark values. Keeping current bookmark.")
            value = current_bookmark

        if "bookmarks" not in state:
            state["bookmarks"] = {}
        if stream not in state["bookmarks"]:
            state["bookmarks"][stream] = {}
        state["bookmarks"][stream][bookmark_key] = value

        return state

    def _to_utc_datetime(self, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, (int, float)):
            return datetime.utcfromtimestamp(value).replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            dt = parser.parse(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        LOGGER.warning(f"Unsupported timestamp type: {type(value)}")
        return None

    def append_times_to_dates(self, record: Dict) -> Dict:
        return record

    def sync(self, state: Dict, transformer: Transformer, parent_obj: Dict = None) -> Dict:
        """Abstract implementation for `type: Incremental` stream."""
        bookmark_value = self.get_bookmark(state, self.tap_stream_id)
        bookmark_date = self._to_utc_datetime(bookmark_value)

        if bookmark_date is None:
            LOGGER.error("Invalid bookmark date, using start_date")
            bookmark_date = self._to_utc_datetime(self.client.config["start_date"])

        current_max_bookmark_date = bookmark_date
        self.update_params(updated_since=bookmark_date.isoformat())
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(record, self.schema, self.metadata)
                self.append_times_to_dates(transformed_record)

                record_value = transformed_record.get(self.replication_keys[0])
                record_timestamp = self._to_utc_datetime(record_value)

                if record_timestamp is None:
                    LOGGER.warning(f"Skipping record with invalid {self.replication_keys[0]}: {record_value}")
                    continue

                if record_timestamp >= bookmark_date:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    try:
                        current_max_bookmark_date = max(current_max_bookmark_date, record_timestamp)
                    except TypeError:
                        LOGGER.warning("Timestamp comparison failed, keeping current bookmark")

                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

            state = self.update_bookmark_state(  # pylint: disable=E1121
                state=state,
                stream=self.tap_stream_id,
                key=None,
                value=current_max_bookmark_date.isoformat()
            )
            return counter.value

    class FullTableStream(BaseStream):
        """Base Class for FullTable Stream."""

        replication_keys = []

        def sync(
            self,
            state: Dict,
            transformer: Transformer,
            parent_obj: Dict = None,
        ) -> Dict:
            """Abstract implementation for `type: Fulltable` stream."""
            self.url_endpoint = self.get_url_endpoint(parent_obj)
            with metrics.record_counter(self.tap_stream_id) as counter:
                for record in self.get_records():
                    record = self.modify_object(record, parent_obj)
                    transformed_record = transformer.transform(
                        record, self.schema, self.metadata
                    )
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

                return counter.value
