import boto3
from singer import metadata
import singer
from tap_dynamodb.sync_strategies.full_table import sync_full_table
from tap_dynamodb.sync_strategies.log_based import sync_log_based, has_stream_aged_out


LOGGER = singer.get_logger()

def sync_stream(config, state, stream):
    table_name = stream['tap_stream_id']

    md_map = metadata.to_map(stream['metadata'])
    replication_method = metadata.get(md_map, (), 'replication-method')
    key_properties = metadata.get(md_map, (), 'table-key-properties')

    # TODO Clear state on replication method change?

    # write state message with currently_syncing bookmark
    state = singer.set_currently_syncing(state, table_name)
    singer.write_state(state)

    singer.write_message(singer.SchemaMessage(
        stream=table_name,
        schema=stream['schema'],
        key_properties=key_properties))

    if replication_method == 'FULL_TABLE':
        LOGGER.info("Syncing full table for stream: %s", table_name)
        sync_full_table(config, state, stream)
    elif replication_method == 'LOG_BASED':
        LOGGER.info("Syncing log based for stream: %s", table_name)
        if has_stream_aged_out(table_name):
            LOGGER.info("Clearing state because stream has aged out")
            state.get('bookmarks', {}).pop(table_name)

        if not singer.get_bookmark(state, table_name, 'initial_full_table_complete'):
            msg = 'Must complete full table sync before replicating from dynamodb streams for %s'
            LOGGER.info(msg, table_name)

            # only mark latest sequence numbers in dynamo streams on first sync so
            # tap has a starting point after the full table sync
            if not singer.get_bookmark(state, table_name, 'version'):
                latest_sequence_numbers = sync_log_based.get_latest_seq_numbers(config, stream)
                state = singer.write_bookmark(state, table_name, 'shard_seq_numbers', latest_sequence_numbers)

            sync_full_table(config, state, stream)

        sync_log_based(config, state, stream)
    else:
        LOGGER.info('Unknown replication method: %s for stream: %s', replication_method, table_name)
