import time
import singer
from singer import metadata
import backoff
from botocore.exceptions import ConnectTimeoutError, ReadTimeoutError
from tap_dynamodb.deserialize import Deserializer
from tap_dynamodb import dynamodb

LOGGER = singer.get_logger()


def scan_table(table_name, projection, expression, last_evaluated_key, config):
    '''
    Get all the records of the table by using `scan()` method with projection expression parameters
    '''
    scan_params = {
        'TableName': table_name,
        'Limit': 1000
    }

    # add the projection expression in the parameters to the `scan`
    if projection is not None and projection != '':
        scan_params['ProjectionExpression'] = projection
    if expression:
        # Add `ExpressionAttributeNames` parameter for reserved word.
        scan_params['ExpressionAttributeNames'] = dynamodb.decode_expression(expression)
    if last_evaluated_key is not None:
        scan_params['ExclusiveStartKey'] = last_evaluated_key

    client = dynamodb.get_client(config)
    has_more = True
    LOGGER.info('Scanning table %s with params:', table_name)
    for key, value in scan_params.items():
        LOGGER.info('\t%s = %s', key, value)

    while has_more:
        result = client.scan(**scan_params)
        yield result

        if result.get('LastEvaluatedKey'):
            scan_params['ExclusiveStartKey'] = result['LastEvaluatedKey']

        has_more = result.get('LastEvaluatedKey', False)

# Backoff for both ReadTimeout and ConnectTimeout error for 5 times
@backoff.on_exception(backoff.expo,
                      (ReadTimeoutError, ConnectTimeoutError),
                      max_tries=5,
                      factor=2)
def sync(config, state, stream):
    table_name = stream['tap_stream_id']

    # before writing the table version to state, check if we had one to begin with
    first_run = singer.get_bookmark(state, table_name, 'version') is None

    # last run was interrupted if there is a last_id_fetched bookmark
    was_interrupted = singer.get_bookmark(state,
                                          table_name,
                                          'last_evaluated_key') is not None

    # pick a new table version if last run wasn't interrupted
    if was_interrupted:
        stream_version = singer.get_bookmark(state, table_name, 'version')
    else:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  table_name,
                                  'version',
                                  stream_version)
    singer.write_state(state)

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if first_run:
        singer.write_version(table_name, stream_version)

    last_evaluated_key = singer.get_bookmark(state,
                                             table_name,
                                             'last_evaluated_key')

    md_map = metadata.to_map(stream['metadata'])
    projection = metadata.get(md_map, (), 'tap-mongodb.projection')

    # An expression attribute name is a placeholder that one uses in an Amazon DynamoDB expression as an alternative to an actual attribute name.
    # Sometimes it might need to write an expression containing an attribute name that conflicts with a DynamoDB reserved word.
    # For example, table `A` contains the field `Comment` but `Comment` is a reserved word. So, it fails during fetch.
    expression = metadata.get(md_map, (), 'tap-dynamodb.expression-attributes')

    rows_saved = 0

    deserializer = Deserializer()
    for result in scan_table(table_name, projection, expression, last_evaluated_key, config):
        for item in result.get('Items', []):
            rows_saved += 1
            # TODO: Do we actually have to put the item we retreive from
            # dynamo into a map before we can deserialize?
            record = deserializer.deserialize_item(item)
            record_message = singer.RecordMessage(stream=table_name,
                                                  record=record,
                                                  version=stream_version)

            singer.write_message(record_message)
        if result.get('LastEvaluatedKey'):
            state = singer.write_bookmark(state, table_name, 'last_evaluated_key', result.get('LastEvaluatedKey'))
            singer.write_state(state)

    state = singer.clear_bookmark(state, table_name, 'last_evaluated_key')

    state = singer.write_bookmark(state,
                                  table_name,
                                  'initial_full_table_complete',
                                  True)

    singer.write_state(state)

    singer.write_version(table_name, stream_version)

    return rows_saved
