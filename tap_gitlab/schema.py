import os
import json
import singer
from typing import Dict, Tuple
from singer import metadata
from tap_gitlab.streams import STREAMS

LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    """Get the absolute path for the schema files."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema_references() -> Dict:
    """Load the schema files from the schema/shared folder and return reference map."""
    shared_schema_path = get_abs_path("schemas/shared")
    shared_file_names = []

    if os.path.exists(shared_schema_path):
        shared_file_names = [
            f for f in os.listdir(shared_schema_path)
            if os.path.isfile(os.path.join(shared_schema_path, f))
        ]

    refs = {}
    for shared_schema_file in shared_file_names:
        with open(os.path.join(shared_schema_path, shared_schema_file)) as data_file:
            refs["shared/" + shared_schema_file] = json.load(data_file)

    return refs


def get_schemas() -> Tuple[Dict, Dict]:
    """Load all schemas and generate metadata with resolved references."""
    schemas = {}
    field_metadata = {}
    refs = load_schema_references()

    for stream_name, stream_obj in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        if not os.path.exists(schema_path):
            LOGGER.warning("Schema file not found for stream: %s", stream_name)
            continue

        with open(schema_path) as file:
            schema = json.load(file)

        schemas[stream_name] = schema
        schema = singer.resolve_schema_references(schema, refs)

        # Safely resolve replication keys
        replication_keys = []
        try:
            # Get raw value
            raw_replication_keys = getattr(stream_obj, "replication_keys", [])

            # Check if it's a class-level @property
            stream_cls_attr = type(stream_obj).__dict__.get("replication_keys", None)
            if isinstance(stream_cls_attr, property):
                replication_keys = stream_cls_attr.__get__(stream_obj)

            # Else, only accept list/tuple
            elif isinstance(raw_replication_keys, (list, tuple)):
                replication_keys = raw_replication_keys

            # Do not call anything
            else:
                replication_keys = []

        except Exception as exc:
            LOGGER.error("Failed to resolve replication_keys for stream '%s': %s", stream_name, exc)
            replication_keys = []

        replication_keys = replication_keys or []

        # Generate standard metadata list
        try:
            mdata_list = metadata.get_standard_metadata( # type: ignore[attr-defined]
                schema=schema,
                key_properties=getattr(stream_obj, "key_properties", []),
                valid_replication_keys=replication_keys,
                replication_method=getattr(stream_obj, "replication_method", None),
            )
        except AttributeError:
            LOGGER.critical("metadata.get_standard_metadata not found — install the correct singer fork.")
            raise

        mdata = metadata.to_map(mdata_list)

        # Add selected: true to top-level
        mdata = metadata.write(mdata, (), "selected", True)

        # Set inclusion=automatic for replication keys
        properties = schema.get("properties", {})
        for field_name in properties.keys():
            if field_name in replication_keys:
                mdata = metadata.write(
                    mdata, ("properties", field_name), "inclusion", "automatic"
                )

        field_metadata[stream_name] = metadata.to_list(mdata)

    return schemas, field_metadata
