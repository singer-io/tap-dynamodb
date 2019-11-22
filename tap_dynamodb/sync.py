import boto3
from singer import metadata
import singer
from tap_dynamodb.sync_strategies.full_table import sync_full_table
from tap_dynamodb.sync_strategies.log_based import sync_log_based


LOGGER = singer.get_logger()

def sync_stream(config, state, stream):
    tap_stream_id = stream['tap_stream_id']

    md_map = metadata.to_map(stream['metadata'])
    replication_method = metadata.get(md_map, (), 'replication-method')
    key_properties = metadata.get(md_map, (), 'table-key-properties')

    # TODO Clear state on replication method change?

    # write state message with currently_syncing bookmark
    state = singer.set_currently_syncing(state, tap_stream_id)
    singer.write_state(state)

    singer.write_message(singer.SchemaMessage(
        stream=tap_stream_id,
        schema=stream['schema'],
        key_properties=key_properties))

    if replication_method == 'FULL_TABLE':
        LOGGER.info("Syncing full table for stream: %s", tap_stream_id)
        sync_full_table(config, state, stream)
    elif replication_method == 'LOG_BASED':
        LOGGER.info("Syncing log based for stream: %s", tap_stream_id)
        sync_log_based(config, state, stream)
    else:
        LOGGER.info('Unknown replication method: %s for stream: %s', replication_method, tap_stream_id)
