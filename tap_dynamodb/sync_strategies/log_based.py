import datetime
from singer import metadata
import singer

from tap_dynamodb import dynamodb
from tap_dynamodb import deserialize

LOGGER = singer.get_logger()
WRITE_STATE_PERIOD = 1000

SDC_DELETED_AT = "_sdc_deleted_at"


def get_shards(streams_client, stream_arn):
    '''
    Yields closed shards.

    We only yield closed shards because it is
    impossible to tell if an open shard has any more records or if you
    will infinitely loop over the lastEvaluatedShardId
    '''

    params = {
        'StreamArn': stream_arn
    }

    has_more = True

    while has_more:
        stream_info = streams_client.describe_stream(**params)['StreamDescription']

        for shard in stream_info['Shards']:
            # See
            # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_DescribeStream.html
            # for documentation on how to identify closed shards
            # Closed shards all have an EndingSequenceNumber
            if shard['SequenceNumberRange'].get('EndingSequenceNumber'):
                yield shard

        last_evaluated_shard_id = stream_info.get('LastEvaluatedShardId')
        has_more = last_evaluated_shard_id is not None

        if has_more:
            params['ExclusiveStartShardId'] = last_evaluated_shard_id


def get_shard_records(streams_client, stream_arn, shard, sequence_number):
    '''
    This should only be called on closed shards. Calling this on an open
    shard will lead to an infinite loop

    Yields the records on a shard.
    '''
    if sequence_number:
        iterator_type = 'AFTER_SEQUENCE_NUMBER'
    else:
        iterator_type = 'TRIM_HORIZON'

    params = {
        'StreamArn': stream_arn,
        'ShardId': shard['ShardId'],
        'ShardIteratorType': iterator_type
    }

    if sequence_number:
        params['SequenceNumber'] = sequence_number

    shard_iterator = streams_client.get_shard_iterator(**params)['ShardIterator']

    # This will loop indefinitely if called on open shards
    while shard_iterator:
        records = streams_client.get_records(ShardIterator=shard_iterator, Limit=1000)

        for record in records['Records']:
            yield record

        shard_iterator = records.get('NextShardIterator')


def sync_shard(shard, seq_number_bookmarks, streams_client, stream_arn, projection, deserializer, table_name, stream_version, state):
    seq_number = seq_number_bookmarks.get(shard['ShardId'])

    rows_synced = 0

    for record in get_shard_records(streams_client, stream_arn, shard, seq_number):
        if record['eventName'] == 'REMOVE':
            record_message = deserializer.deserialize_item(record['dynamodb']['Keys'])
            record_message[SDC_DELETED_AT] = singer.utils.strftime(record['dynamodb']['ApproximateCreationDateTime'])
        else:
            record_message = deserializer.deserialize_item(record['dynamodb'].get('NewImage'))
            if record_message is None:
                LOGGER.fatal('Dynamo stream view type must be either "NEW_IMAGE" "NEW_AND_OLD_IMAGES"')
                raise RuntimeError('Dynamo stream view type must be either "NEW_IMAGE" "NEW_AND_OLD_IMAGES"')
            if projection is not None:
                try:
                    record_message = deserializer.apply_projection(record_message, projection)
                except:
                    LOGGER.fatal("Projection failed to apply: %s", projection)
                    raise RuntimeError('Projection failed to apply: {}'.format(projection))

        record_message = singer.RecordMessage(stream=table_name,
                                              record=record_message,
                                              version=stream_version)
        singer.write_message(record_message)

        rows_synced += 1

        seq_number_bookmarks[shard['ShardId']] = record['dynamodb']['SequenceNumber']
        state = singer.write_bookmark(state, table_name, 'shard_seq_numbers', seq_number_bookmarks)

        # Every 100 rows write the state
        if rows_synced % 100 == 0:
            singer.write_state(state)

    singer.write_state(state)
    return rows_synced


def sync(config, state, stream):
    table_name = stream['tap_stream_id']

    client = dynamodb.get_client(config)
    streams_client = dynamodb.get_stream_client(config)

    md_map = metadata.to_map(stream['metadata'])
    projection = metadata.get(md_map, (), 'tap-mongodb.projection')
    if projection is not None:
        projection = [x.strip().split('.') for x in projection.split(',')]

    # Write activate version message
    stream_version = singer.get_bookmark(state, table_name, 'version')
    singer.write_version(table_name, stream_version)

    table = client.describe_table(TableName=table_name)['Table']
    stream_arn = table['LatestStreamArn']

    # Stores a dictionary of shardId : sequence_number for a shard. Should
    # only store sequence numbers for closed shards that have not been
    # fully synced
    seq_number_bookmarks = singer.get_bookmark(state, table_name, 'shard_seq_numbers')
    if not seq_number_bookmarks:
        seq_number_bookmarks = dict()

    # Get the list of closed shards which we have fully synced. These
    # are removed after performing a sync and not seeing the shardId
    # returned by get_shards() because at that point the shard has been
    # killed by DynamoDB and will not be returned anymore
    finished_shard_bookmarks = singer.get_bookmark(state, table_name, 'finished_shards')
    if not finished_shard_bookmarks:
        finished_shard_bookmarks = list()

    # The list of shardIds we found this sync. Is used to determine which
    # finished_shard_bookmarks to kill
    found_shards = []

    deserializer = deserialize.Deserializer()

    rows_synced = 0

    for shard in get_shards(streams_client, stream_arn):
        found_shards.append(shard['ShardId'])
        # Only sync shards which we have not fully synced already
        if shard['ShardId'] not in finished_shard_bookmarks:
            rows_synced += sync_shard(shard, seq_number_bookmarks,
                                      streams_client, stream_arn, projection, deserializer,
                                      table_name, stream_version, state)

        # Now that we have fully synced the shard, move it from the
        # shard_seq_numbers to finished_shards.
        finished_shard_bookmarks.append(shard['ShardId'])
        state = singer.write_bookmark(state, table_name, 'finished_shards', finished_shard_bookmarks)

        if seq_number_bookmarks.get(shard['ShardId']):
            seq_number_bookmarks.pop(shard['ShardId'])
            state = singer.write_bookmark(state, table_name, 'shard_seq_numbers', seq_number_bookmarks)

        singer.write_state(state)

    for shardId in finished_shard_bookmarks:
        if shardId not in found_shards:
            # Remove this shard because its no longer appearing when we query for get_shards
            finished_shard_bookmarks.remove(shardId)
            state = singer.write_bookmark(state, table_name, 'finished_shards', finished_shard_bookmarks)

    singer.write_state(state)

    return rows_synced


def has_stream_aged_out(state, table_name):
    '''
    Uses the success_timestamp on the stream to determine if we have
    successfully synced the stream in the last 19 hours 30 minutes.

    This uses 19 hours and 30 minutes because records are guaranteed to be
    available on a shard for 24 hours, but we only process closed shards
    and shards close after 4 hours. 24-4 = 20 and we gave 30 minutes of
    wiggle room because I don't trust AWS.
    See https://aws.amazon.com/blogs/database/dynamodb-streams-use-cases-and-design-patterns/
    '''
    current_time = singer.utils.now()

    success_timestamp = singer.get_bookmark(state, table_name, 'success_timestamp')

    # If we have no success_timestamp then we have aged out
    if not success_timestamp:
        return True

    time_span = current_time - singer.utils.strptime_to_utc(success_timestamp)

    # If it has been > than 19h30m since the last successful sync of this
    # stream then we consider the stream to be aged out
    return time_span > datetime.timedelta(hours=19, minutes=30)


def get_initial_bookmarks(config, state, table_name):
    '''
    Returns the state including all bookmarks necessary for the initial
    full table sync
    '''
    # TODO This can be improved by getting the max sequence number in the
    # open shards and include those in the seq_number bookmark


    client = dynamodb.get_client(config)
    streams_client = dynamodb.get_stream_client(config)

    table = client.describe_table(TableName=table_name)['Table']
    stream_arn = table['LatestStreamArn']

    finished_shard_bookmarks = [shard['ShardId'] for shard in get_shards(streams_client, stream_arn)]
    state = singer.write_bookmark(state, table_name, 'finished_shards', finished_shard_bookmarks)

    return state
