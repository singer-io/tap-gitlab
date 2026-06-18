import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_gitlab.schema import get_schemas
from tap_gitlab.streams import STREAMS
from tap_gitlab.exceptions import ForbiddenError

LOGGER = singer.get_logger()


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates schemas and field_metadata in place.
    """
    for name, stream_cls in list(STREAMS.items()):
        parent = getattr(stream_cls, "parent", None)
        if name in schemas and parent and parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name, parent,
            )
            schemas.pop(name, None)
            field_metadata.pop(name, None)


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """
    Probe each stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.
    Raises ForbiddenError if no parent streams are accessible.
    """
    inaccessible_streams = [
        stream_name
        for stream_name, stream_cls in STREAMS.items()
        if stream_name in schemas
        and not stream_cls(client=client).check_access()
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise ForbiddenError(
            "No streams are accessible. Ensure the credentials have read permission for at least one stream."
        )
    elif inaccessible_streams:
        LOGGER.warning(
            "These streams have been excluded due to HTTP-Error-Code:403 Forbidden: %s",
            ", ".join(inaccessible_streams),
        )


def discover(client) -> Catalog:
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    Access to each stream is verified using the provided client and streams
    the credentials cannot read are excluded from the returned catalog.
    """
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)

    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error("stream_name: {}".format(stream_name))
            LOGGER.error("type schema_dict: {}".format(type(schema_dict)))
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog
