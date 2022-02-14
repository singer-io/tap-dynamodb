import json
import sys
import time

from terminaltables import AsciiTable
import singer
from singer import metadata
from tap_dynamodb.discover import discover_streams
from tap_dynamodb.dynamodb import setup_aws_client
from tap_dynamodb.sync import sync_stream


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["account_id", "external_id", "role_name", "region_name"]

def do_discover(config):
    '''
    Run the discovery mode for each streams
    '''
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")

def stream_is_selected(mdata):
    '''
    Get the streams which are selected from the catalog
    '''
    return mdata.get((), {}).get('selected', False)

def do_sync(config, catalog, state):
    '''
    Run the sync mode for each streams
    '''
    LOGGER.info('Starting sync.')

    counts = {}
    sync_times = {}
    for stream in catalog['streams']:
        start_time = time.time()
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counts[stream_name] = sync_stream(config, state, stream)
        sync_times[stream_name] = time.time() - start_time
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counts[stream_name])

    LOGGER.info(get_sync_summary(catalog, counts, sync_times))
    LOGGER.info('Done syncing.')

def get_sync_summary(catalog, counts, times):
    '''
    Obtain the sync summary for all the stream tables
    '''
    headers = [['table name',
                'replication method',
                'total records',
                'write speed']]

    rows = []
    for stream_id, stream_count in counts.items():
        stream = [x for x in catalog['streams'] if x['tap_stream_id'] == stream_id][0]
        md_map = metadata.to_map(stream['metadata'])
        replication_method = metadata.get(md_map, (), 'replication-method')

        stream_time = times[stream_id]
        if stream_time == 0:
            stream_time = 0.000001
        row = [stream_id,
               replication_method,
               '{} records'.format(stream_count),
               '{:.1f} records/second'.format(stream_count/stream_time)]
        rows.append(row)

    data = headers + rows
    table = AsciiTable(data, title='Sync Summary')

    return '\n\n' + table.table


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    # TODO Is this the right way to do this? It seems bad
    if not config.get('use_local_dynamo'):
        setup_aws_client(config)

    if args.discover:
        do_discover(args.config)
    elif args.catalog:
        do_sync(config, args.catalog.to_dict(), args.state)


if __name__ == '__main__':
    main()
