from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple, List
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    metadata
)
from dateutil import parser
from datetime import datetime, timezone
import logging

LOGGER = get_logger()


class BaseStream(ABC):
    url_endpoint = ""
    path = ""
    page_size = 0
    next_page_key = ""
    headers = {}
    children = []
    parent = ""
    data_key = None
    parent_bookmark_key = ""

    def __init__(self, client=None, catalog=None):
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync = []
        self.params = {}

    @property
    @abstractmethod
    def tap_stream_id(self):
        pass

    @property
    @abstractmethod
    def replication_method(self):
        pass

    @property
    @abstractmethod
    def replication_keys(self):
        pass

    @property
    @abstractmethod
    def key_properties(self):
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

    def get_records(self):
        self.params[""] = self.page_size
        next_page = 1
        while next_page:
            response = self.client.get(
                self.url_endpoint, self.params, self.headers, self.path
            )
            if isinstance(response, list):
                raw_records = response
                next_page = None
            else:
                raw_records = response.get(self.data_key, [])
                next_page = response.get(self.next_page_key)

            self.params[self.next_page_key] = next_page
            yield from raw_records

    def write_schema(self):
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: {}".format(self.tap_stream_id)
            )
            raise err

    def update_params(self, **kwargs) -> None:
        self.params.update(kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None):
        return record

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        return self.url_endpoint or f"{self.client.base_url}/{self.path}"


class IncrementalStream(BaseStream):
    def get_bookmark(self, state: dict, stream: str, key: Any = None):
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None):
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(state, stream, key or self.replication_keys[0], self.client.config["start_date"])
        try:
            value = max(current_bookmark, value)
        except Exception:
            LOGGER.warning("Failed to compare bookmark values. Keeping current bookmark.")
            value = current_bookmark

        return write_bookmark(
            state, stream, key or self.replication_keys[0], value
        )

    def _to_utc_datetime(self, value):
        """Convert various timestamp formats to UTC datetime"""
        if value is None:
            return None
        if isinstance(value, datetime):
            # Handle existing datetime objects
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, (int, float)):
            # Convert Unix timestamp to UTC datetime
            return datetime.utcfromtimestamp(value).replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            # Parse string and convert to UTC
            dt = parser.parse(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        LOGGER.warning(f"Unsupported timestamp type: {type(value)}")
        return None

    def append_times_to_dates(self, record: Dict):
        return record

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        # Convert bookmark to UTC datetime
        bookmark_value = self.get_bookmark(state, self.tap_stream_id)
        bookmark_date = self._to_utc_datetime(bookmark_value)
        
        # Fallback to start_date if conversion fails
        if bookmark_date is None:
            LOGGER.error("Invalid bookmark date, using start_date")
            bookmark_date = self._to_utc_datetime(self.client.config["start_date"])
        
        current_max_bookmark_date = bookmark_date
        self.update_params(updated_since=bookmark_date.isoformat())
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                self.append_times_to_dates(transformed_record)

                # Get and convert record timestamp
                record_value = transformed_record.get(self.replication_keys[0])
                record_timestamp = self._to_utc_datetime(record_value)
                
                # Skip record if timestamp conversion fails
                if record_timestamp is None:
                    LOGGER.warning(
                        f"Skipping record with invalid {self.replication_keys[0]}: {record_value}"
                    )
                    continue

                # Compare UTC datetimes
                if record_timestamp >= bookmark_date:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    # Update max bookmark safely
                    try:
                        current_max_bookmark_date = max(
                            current_max_bookmark_date, record_timestamp
                        )
                    except TypeError:
                        LOGGER.warning("Timestamp comparison failed, keeping current bookmark")
                    
                    # Sync children
                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

            # Store bookmark as ISO string
            state = self.write_bookmark(
                state,
                self.tap_stream_id,
                value=current_max_bookmark_date.isoformat()
            )
            return counter.value


class FullTableStream(BaseStream):
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value
