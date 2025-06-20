import singer
from typing import Dict
from tap_gitlab.streams import STREAMS
from tap_gitlab.client import Client

LOGGER = singer.get_logger()

def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """Update currently_syncing in state and write it."""
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)

def write_schema(stream, client, streams_to_sync, catalog) -> None:
    """Write schema for stream and its children if applicable."""
    if stream.is_selected():
        stream.write_schema()

    for child in getattr(stream, "children", []):
        try:
            child_catalog = catalog.get_stream(child)
            if not child_catalog:
                LOGGER.warning(f"Skipping child stream {child}: not in catalog.")
                continue
            child_obj = STREAMS[child](client, child_catalog)
            write_schema(child_obj, client, streams_to_sync, catalog)
            if hasattr(stream, "child_to_sync"):
                stream.child_to_sync.append(child_obj)
        except Exception as e:
            LOGGER.error(f"Failed to initialize child stream {child}: {str(e)}")

def sync(client: Client, config: Dict, catalog: singer.Catalog, state) -> None:
    """Sync selected streams from catalog."""
    streams_to_sync = []
    for stream in catalog.get_selected_streams(state):
        streams_to_sync.append(stream.tap_stream_id)

    LOGGER.info("selected_streams: {}".format(streams_to_sync))

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: {}".format(last_stream))

    with singer.Transformer() as transformer:
        for stream_name in streams_to_sync:
            try:
                stream_catalog = catalog.get_stream(stream_name)
                if not stream_catalog:
                    LOGGER.critical(f"Stream '{stream_name}' missing in catalog. Skipping.")
                    continue

                stream = STREAMS[stream_name](client, stream_catalog)

                # If stream has a parent and the parent is not in the catalog, skip
                if getattr(stream, "parent", None):
                    parent_name = stream.parent
                    if parent_name not in streams_to_sync:
                        LOGGER.warning(f"Skipping child stream '{stream_name}' since parent '{parent_name}' not selected.")
                        continue

                write_schema(stream, client, streams_to_sync, catalog)

                LOGGER.info(f"START Syncing: {stream_name}")
                update_currently_syncing(state, stream_name)
                total_records = stream.sync(state=state, transformer=transformer)
                update_currently_syncing(state, None)

                LOGGER.info(f"FINISHED Syncing: {stream_name}, total_records: {total_records}")
            except Exception as e:
                LOGGER.critical(f"Exception during syncing stream '{stream_name}': {str(e)}")
