import boto3
import time

import singer
from singer import metadata
from tap_dynamodb import dynamodb
from tap_dynamodb.deserialize import Deserializer

LOGGER = singer.get_logger()

def scan_table(table_name, projection, last_evaluated_key, config):
    scan_params = {
        'TableName': table_name,
        'Limit': 1000
    }

    if projection is not None:
        scan_params['ProjectionExpression'] = projection
    if last_evaluated_key is not None:
        scan_params['ExclusiveStartKey'] = last_evaluated_key

    client = dynamodb.get_client(config)
    has_more = True

    while has_more:
        result = client.scan(**scan_params)
        yield result

        if result.get('LastEvaluatedKey'):
            scan_params['ExclusiveStartKey'] = result['LastEvaluatedKey']

        has_more = result.get('LastEvaluatedKey', False)

def sync_full_table(config, state, stream):

    #before writing the table version to state, check if we had one to begin with
    first_run = singer.get_bookmark(state, stream['tap_stream_id'], 'version') is None

    # last run was interrupted if there is a last_id_fetched bookmark
    was_interrupted = singer.get_bookmark(state,
                                          stream['tap_stream_id'],
                                          'last_evaluated_key') is not None

    #pick a new table version if last run wasn't interrupted
    if was_interrupted:
        stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    else:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  stream_version)
    singer.write_state(state)

    activate_version_message = singer.ActivateVersionMessage(
        stream=stream['tap_stream_id'],
        version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if first_run:
        singer.write_message(activate_version_message)


    last_evaluated_key = singer.get_bookmark(state,
                                             stream['tap_stream_id'],
                                             'last_evaluated_key')

    md_map = metadata.to_map(stream['metadata'])
    projection = metadata.get(md_map, (), 'tap-mongodb.projection')

    client = dynamodb.get_client(config)
    rows_saved = 0

    deserializer = Deserializer()
    for result in scan_table(stream['tap_stream_id'], projection, last_evaluated_key, config):
        for item in result.get('Items', []):
            rows_saved += 1
            # TODO: Do we actually have to put the item we retreive from
            # dynamo into a map before we can deserialize?
            record_message = deserializer.deserialize_item(item)
            singer.write_record(stream['tap_stream_id'], record_message)
        if result.get('LastEvaluatedKey'):
            state = singer.write_bookmark(state, stream['tap_stream_id'], 'last_evaluated_key', result.get('LastEvaluatedKey'))
            singer.write_state(state)

    state = singer.clear_bookmark(state, stream['tap_stream_id'], 'last_evaluated_key')

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'initial_full_table_complete',
                                  True)

    singer.write_state(state)

    singer.write_message(activate_version_message)

    LOGGER.info('Synced {} records for {}'.format(rows_saved, stream['tap_stream_id']))
