import boto3
from singer import metadata
import singer

from tap_dynamodb import dynamodb
from tap_dynamodb import deserialize

LOGGER = singer.get_logger()
WRITE_STATE_PERIOD = 1000

SDC_DELETED_AT = "_sdc_deleted_at"

def get_shards(streams_client, stream_arn, open_shards_only=False):
    params = {
        'StreamArn': stream_arn
    }

    has_more = True

    while has_more:
        stream_info = streams_client.describe_stream(**params)['StreamDescription']

        for shard in stream_info['Shards']:
            if open_shards_only:
                if not shard['SequenceNumberRange'].get('EndingSequenceNumber'):
                    yield shard
            else:
                yield shard

        last_evaluated_shard_id = stream_info.get('LastEvaluatedShardId')
        has_more = last_evaluated_shard_id is not None

        if has_more:
            params['ExclusiveStartShardId'] = last_evaluated_shard_id


def get_shard_records(streams_client, stream_arn, shard, shard_iterator_type, sequence_number = ''):
    params = {
        'StreamArn': stream_arn,
        'ShardId': shard['ShardId'],
        'ShardIteratorType': shard_iterator_type
    }

    if shard_iterator_type == 'AFTER_SEQUENCE_NUMBER':
        params['SequenceNumber'] = sequence_number

    shard_iterator = streams_client.get_shard_iterator(**params)['ShardIterator']

    limit = 1000
    has_more = True

    while has_more:

        records = streams_client.get_records(ShardIterator=shard_iterator, Limit=limit)

        for record in records['Records']:
            yield record

        shard_iterator = records.get('NextShardIterator')

        has_more = len(records['Records']) == limit and records.get('NextShardIterator')

def get_latest_seq_numbers(config, stream):
    '''
    returns {shardId: sequence_number}
    '''

    client = dynamodb.get_client(config)
    streams_client = dynamodb.get_stream_client(config)
    table_name = stream['tap_stream_id']

    table = client.describe_table(TableName=table_name)['Table']
    stream_arn = table['LatestStreamArn']

    sequence_number_bookmarks = {}
    LOGGER.info('Retreiving records from stream %s', stream_arn)
    for shard in get_shards(streams_client, stream_arn, True):
        LOGGER.info('\tRetreiving records from shard %s', shard['ShardId'])
        has_records = False
        for record in get_shard_records(streams_client, stream_arn, shard, 'TRIM_HORIZON'):
            has_records = True
            # Get the final record
        if has_records:
            sequence_number_bookmarks[shard['ShardId']] = record['dynamodb']['SequenceNumber']

    return sequence_number_bookmarks


def sync_shard(shard, seq_number_bookmarks, streams_client, stream_arn, projection, deserializer, table_name, stream_version, state):
    seq_number = seq_number_bookmarks.get(shard['ShardId'])
    if seq_number:
        iterator_type = 'AFTER_SEQUENCE_NUMBER'
    else:
        iterator_type = 'TRIM_HORIZON'

    rows_synced = 0

    for record in get_shard_records(streams_client, stream_arn, shard, iterator_type, seq_number):
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

    if rows_synced > 0:
        singer.write_state(state)

    return rows_synced


def sync_log_based(config, state, stream):
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
    # only store sequence numbers for open shards, as closed shards should
    # be put into finished_shard_bookmarks
    seq_number_bookmarks = singer.get_bookmark(state, table_name, 'shard_seq_numbers')

    # Store the list of closed shards which we have fully synced. These
    # are removed after performing a sync and not seeing the shardId
    # returned by get_shards() because at that point the shard has been
    # killed by DynamoDB and will not be returned anymore
    finished_shard_bookmarks = singer.get_bookmark(state, table_name, 'finished_shards')
    if finished_shard_bookmarks is None:
        finished_shard_bookmarks = []

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

        # If the shard we just finished syncing is closed (i.e. has an
        # EndingSequenceNumber), remove it from the seq_number_bookmark
        # and put it into the finished_shards bookmark
        if shard['SequenceNumberRange'].get('EndingSequenceNumber'):
            # Must check if the bookmark exists because if a shard has 0
            # records we will never set a bookmark for the shard
            if seq_number_bookmarks.get(shard['ShardId']):
                seq_number_bookmarks.pop(shard['ShardId'])
                finished_shard_bookmarks.append(shard['ShardId'])
                state = singer.write_bookmark(state, table_name, 'finished_shards', finished_shard_bookmarks)
                state = singer.write_bookmark(state, table_name, 'shard_seq_numbers', seq_number_bookmarks)
                singer.write_state(state)

    for shard in finished_shard_bookmarks:
        if shard not in found_shards:
            # Remove this shard because its no longer appearing when we query for get_shards
            finished_shard_bookmarks.remove(shard)
            state = singer.write_bookmark(state, table_name, 'finished_shards', finished_shard_bookmarks)

    singer.write_state(state)

    return rows_synced

def has_stream_aged_out(config, state, stream):
    client = dynamodb.get_client(config)
    streams_client = dynamodb.get_stream_client(config)
    table = client.describe_table(TableName=stream['tap_stream_id'])['Table']
    stream_arn = table['LatestStreamArn']

    seq_number_bookmarks = singer.get_bookmark(state, stream['tap_stream_id'], 'shard_seq_numbers')

    # If we do not have sequence number bookmarks then check the
    # finished_shard bookmark and see if we have any of the previously
    # finished shards still being returned by DynamoDB. If we do then we
    # have not yet aged out
    if seq_number_bookmarks is None or seq_number_bookmarks == {}:
        # If we have only closed shard bookmarks and one of those shards
        # are returned to us by get_shards then we have not missed any
        # records
        finished_shard_bookmarks = singer.get_bookmark(state, stream['tap_stream_id'], 'finished_shards')
        if finished_shard_bookmarks is None:
           return True

        closed_shards =  (x['ShardId'] for x in get_shards(streams_client, stream_arn) if x['SequenceNumberRange'].get('EndingSequenceNumber', False))

        for shard in closed_shards:
            if shard in finished_shard_bookmarks:
                return False
        return True
    else:
        # If we have a bookmark for an open shard and that shard is not
        # returned to us by DynamoDB then we have missed the remainder of
        # the shard. Similarly if the shard's earliest sequence number is
        # after our bookmark then we have missed records on the shard
        shard_beg_seq_num =  {x['ShardId']: x['SequenceNumberRange']['StartingSequenceNumber'] for x in get_shards(streams_client, stream_arn)}

        for shard_id, shard_bookmark in seq_number_bookmarks.items():
            if shard_beg_seq_num.get(shard_id) is None or shard_beg_seq_num[shard_id] > shard_bookmark:
                return True
        return False
