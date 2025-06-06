import os
import json
import singer
from typing import Dict, Tuple
from singer import metadata
from tap_sample.streams import STREAMS

LOGGER = singer.get_logger()

def get_abs_path(path: str) -> str:
    """
    Get the absolute path for the schema files.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema_references() -> Dict:
    """
    Load the schema files from the schema/shared folder and return reference map.
    """
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
    """
    Load all schemas and generate metadata with resolved references.
    Returns:
        schemas: Dict of stream name to schema
        field_metadata: Dict of stream name to metadata entries
    """
    schemas = {}
    field_metadata = {}
    refs = load_schema_references()

    for stream_name, stream_obj in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        if not os.path.exists(schema_path):
            LOGGER.warning(f"Schema file not found for stream: {stream_name}")
            continue

        with open(schema_path) as file:
            schema = json.load(file)

        schemas[stream_name] = schema
        schema = singer.resolve_schema_references(schema, refs)

        # Safely resolve replication keys
        try:
            replication_keys = getattr(stream_obj, "replication_keys", [])
            if isinstance(replication_keys, property):
                LOGGER.warning(f"'replication_keys' is a @property in stream: {stream_name}")
                replication_keys = replication_keys.fget(stream_obj)
            elif callable(replication_keys):
                replication_keys = replication_keys()
        except Exception as e:
            LOGGER.error(f"Failed to resolve replication_keys for stream '{stream_name}': {e}")
            replication_keys = []

        replication_keys = replication_keys or []

        # Generate standard metadata list
        mdata_list = metadata.get_standard_metadata(
            schema=schema,
            key_properties=getattr(stream_obj, "key_properties", []),
            valid_replication_keys=replication_keys,
            replication_method=getattr(stream_obj, "replication_method", None),
        )

        # Convert to metadata map
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

        # Save metadata as list for CatalogEntry
        field_metadata[stream_name] = metadata.to_list(mdata)

    return schemas, field_metadata
