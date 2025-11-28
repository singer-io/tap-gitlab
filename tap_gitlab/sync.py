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

    for child in stream.children:
        child_obj = STREAMS[child](client, catalog.get_stream(child))
        write_schema(child_obj, client, streams_to_sync, catalog)
        if child in streams_to_sync:
            stream.child_to_sync.append(child_obj)

def sync(client: Client, config: Dict, catalog: singer.Catalog, state) -> None:
    """Sync selected streams from catalog."""
    streams_to_sync = []
    for stream in catalog.get_selected_streams(state):
        streams_to_sync.append(stream.tap_stream_id)
    LOGGER.info("selected_streams: {}".format(streams_to_sync))

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: {}".format(last_stream))

    with singer.Transformer() as transformer:
        # Check if we need to skip projects independent sync
        should_skip_projects = "projects" in streams_to_sync and "groups" in streams_to_sync

        for stream_name in streams_to_sync:
            stream = STREAMS[stream_name](client, catalog.get_stream(stream_name))
            # Skip projects if groups is also selected - groups will handle projects sync
            if stream_name == "projects" and should_skip_projects:
                # Check if projects is actually selected or just added because of children
                if stream.is_selected():
                    LOGGER.info("Skipping projects independent sync - will be synced by groups")
                    continue
                else:
                    LOGGER.info("Projects not selected but children are - will sync as parent")
                    # Let it continue to sync as a parent for its children

            if stream.parent:
                if stream.parent not in streams_to_sync:
                    streams_to_sync.append(stream.parent)
                continue

            write_schema(stream, client, streams_to_sync, catalog)
            LOGGER.info(f"START Syncing: {stream_name}")
            update_currently_syncing(state, stream_name)

            if stream_name == "groups":
                total_records = stream.sync(
                    state=state,
                    transformer=transformer,
                    streams_to_sync=streams_to_sync,
                    catalog=catalog
                )
            else:
                total_records = stream.sync(state=state, transformer=transformer)

            update_currently_syncing(state, None)
            LOGGER.info(f"FINISHED Syncing: {stream_name}, total_records: {total_records}")
