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

        if stream_info.get('LastEvaluatedShardId'):
            params['ExclusiveStartShardId'] = last_evaluated_shard_id

        has_more = stream_info.get('LastEvaluatedShardId', False)

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
    seq_number_bookmarks = singer.get_bookmark(state, table_name, 'shard_seq_numbers')

    deserializer = deserialize.Deserializer()

    rows_saved = 0

    for shard in get_shards(streams_client, stream_arn):
        # check for bookmark
        seq_number = seq_number_bookmarks.get(shard['ShardId'])
        if seq_number:
            iterator_type = 'AFTER_SEQUENCE_NUMBER'
        else:
            iterator_type = 'TRIM_HORIZON'

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
                        LOGGER.fatal("Projection failed to apply: %s", metadata.get(md_map, (), 'tap-mongodb.projection'))
                        raise RuntimeError('Projection failed to apply: {}'.format(metadata.get(md_map, (), 'tap-mongodb.projection')))

            record_message = singer.RecordMessage(stream=table_name,
                                                  record=record_message,
                                                  version=stream_version)
            singer.write_message(record_message)

            rows_saved += 1

            seq_number_bookmarks[shard['ShardId']] = record['dynamodb']['SequenceNumber']
            state = singer.write_bookmark(state, table_name, 'shard_seq_numbers', seq_number_bookmarks)

            if rows_saved % WRITE_STATE_PERIOD == 0:
                singer.write_state(state)

        # If the shard we just finished syncing is closed (i.e. has an
        # EndingSequenceNumber), pop it off
        if shard['SequenceNumberRange'].get('EndingSequenceNumber'):
            # Must check if the bookmark exists because if a shard has 0
            # records we will never set a bookmark for the shard
            if seq_number_bookmarks.get(shard['ShardId']):
                seq_number_bookmarks.pop(shard['ShardId'])
                state = singer.write_bookmark(state, table_name, 'shard_seq_numbers', seq_number_bookmarks)

        singer.write_state(state)

    return rows_saved

def has_stream_aged_out(config, state, stream):
    # TODO Check if the beginning sequence number can move. If it cannot this > check is useless
    seq_number_bookmarks = singer.get_bookmark(state, stream['tap_stream_id'], 'shard_seq_numbers')
    if seq_number_bookmarks is None:
        return False

    client = dynamodb.get_client(config)
    streams_client = dynamodb.get_stream_client(config)
    table = client.describe_table(TableName=stream['tap_stream_id'])['Table']

    stream_arn = table['LatestStreamArn']
    shard_beg_seq_num =  {x['ShardId']: x['SequenceNumberRange']['StartingSequenceNumber'] for x in get_shards(streams_client, stream_arn)}

    for shard_id, shard_bookmark in seq_number_bookmarks.items():
        if shard_beg_seq_num.get(shard_id) is None or shard_beg_seq_num[shard_id] > shard_bookmark:
            return True
    return False
