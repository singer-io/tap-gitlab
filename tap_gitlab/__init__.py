import sys
import json
import singer
from tap_gitlab.client import Client
from tap_gitlab.discover import discover
from tap_gitlab.sync import sync

LOGGER = singer.get_logger()


REQUIRED_CONFIG_KEYS = ["private_token", "start_date", "groups"]


def do_discover():
    """
    Discover and emit the catalog to stdout
    """
    LOGGER.info("Starting discover")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main():
    """
    Run the tap
    """
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    state = {}
    if parsed_args.state:
        state = parsed_args.state

    with Client(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(
                client=client,
                config=parsed_args.config,
                catalog=parsed_args.catalog,
                state=state,
            )


if __name__ == "__main__":
    main()
