import json
import sys
import singer

from singer import metadata
from tap_dynamodb.discover import discover_streams
from tap_dynamodb.client import setup_aws_client

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["account_id", "external_id", "role_name"]

def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def do_sync(config, catalog, state):
    pass
    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    
    # try:
    #     for page in s3.list_files_in_bucket(config['bucket']):
    #         break
    #     LOGGER.warning("I have direct access to the bucket without assuming the configured role.")
    # except:
    #     client.setup_aws_client(config)


    # TODO Is this the right way to do this? It seems bad
    if not config.get("DEBUG_use_local_dynamo"):
        setup_aws_client(config)

    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(config, args.catalog.to_dict(), args.state)


if __name__ == '__main__':
    main()
