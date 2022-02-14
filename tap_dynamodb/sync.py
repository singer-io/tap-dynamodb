from singer import metadata
import singer
from tap_dynamodb.sync_strategies import log_based
from tap_dynamodb.sync_strategies import full_table

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
        rows_saved += full_table.sync(config, state, stream)
    elif replication_method == 'LOG_BASED':
        LOGGER.info("Syncing log based for stream: %s", table_name)

        if log_based.has_stream_aged_out(state, table_name):
            LOGGER.info("Clearing state because stream has aged out")
            state.get('bookmarks', {}).pop(table_name)

        if not singer.get_bookmark(state, table_name, 'initial_full_table_complete'):
            msg = 'Must complete full table sync before replicating from dynamodb streams for %s'
            LOGGER.info(msg, table_name)

            state = log_based.get_initial_bookmarks(config, state, table_name)
            singer.write_state(state)

            rows_saved += full_table.sync(config, state, stream)

        rows_saved += log_based.sync(config, state, stream)
    else:
        LOGGER.info('Unknown replication method: %s for stream: %s', replication_method, table_name)

    state = singer.write_bookmark(state, table_name, 'success_timestamp', singer.utils.strftime(singer.utils.now()))
    singer.write_state(state)

    return rows_saved
