from singer import metadata
import singer
from tap_dynamodb.sync_strategies.full_table import sync_full_table
from tap_dynamodb.sync_strategies.log_based import sync_log_based, has_stream_aged_out, get_latest_seq_numbers


LOGGER = singer.get_logger()

def clear_state_on_replication_change(stream, state):
    md_map = metadata.to_map(stream['metadata'])
    tap_stream_id = stream['tap_stream_id']

    # replication method changed
    current_replication_method = metadata.get(md_map, (), 'replication-method')
    last_replication_method = singer.get_bookmark(state, tap_stream_id, 'last_replication_method')
    if last_replication_method is not None and (current_replication_method != last_replication_method):
        log_msg = 'Replication method changed from %s to %s, will re-replicate entire table %s'
        LOGGER.info(log_msg, last_replication_method, current_replication_method, tap_stream_id)
        state = singer.reset_stream(state, tap_stream_id)

    state = singer.write_bookmark(state, tap_stream_id, 'last_replication_method', current_replication_method)

    return state

def sync_stream(config, state, stream):
    table_name = stream['tap_stream_id']

    md_map = metadata.to_map(stream['metadata'])
    replication_method = metadata.get(md_map, (), 'replication-method')
    key_properties = metadata.get(md_map, (), 'table-key-properties')

    # write state message with currently_syncing bookmark
    state = clear_state_on_replication_change(stream, state)
    state = singer.set_currently_syncing(state, table_name)
    singer.write_state(state)

    singer.write_message(singer.SchemaMessage(
        stream=table_name,
        schema=stream['schema'],
        key_properties=key_properties))

    rows_saved = 0
    if replication_method == 'FULL_TABLE':
        LOGGER.info("Syncing full table for stream: %s", table_name)
        rows_saved += sync_full_table(config, state, stream)
    elif replication_method == 'LOG_BASED':
        LOGGER.info("Syncing log based for stream: %s", table_name)

        if has_stream_aged_out(config, state, stream):
            LOGGER.info("Clearing state because stream has aged out")
            state.get('bookmarks', {}).pop(table_name)

        # TODO Check to see if latest stream ARN has changed and wipe state if so

        if not singer.get_bookmark(state, table_name, 'initial_full_table_complete'):
            msg = 'Must complete full table sync before replicating from dynamodb streams for %s'
            LOGGER.info(msg, table_name)

            # only mark latest sequence numbers in dynamo streams on first sync so
            # tap has a starting point after the full table sync
            if not singer.get_bookmark(state, table_name, 'version'):
                latest_sequence_numbers = get_latest_seq_numbers(config, stream)
                state = singer.write_bookmark(state, table_name, 'shard_seq_numbers', latest_sequence_numbers)

            rows_saved += sync_full_table(config, state, stream)

        rows_saved += sync_log_based(config, state, stream)
    else:
        LOGGER.info('Unknown replication method: %s for stream: %s', replication_method, table_name)

    return rows_saved
