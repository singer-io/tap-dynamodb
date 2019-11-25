import boto3
from singer import metadata
import singer

from tap_dynamodb import dynamodb
from tap_dynamodb import deserialize

LOGGER = singer.get_logger()


def get_shards(streams_client, stream_arn, open_shards_only=False):
    params = {
        'StreamArn': stream_arn
    }

    has_more = True

    while has_more:
        stream_info = streams_client.describe_stream(**params)

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

    shard_iterator = streams_client.get_shard_iterator(**params)

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

    table = dynamodb.describe_table(TableName=table_name)
    stream_arn = table['LatestStreamArn']

    sequence_number_bookmarks = {}
    for shard in get_shards(streams_client, stream_arn, True):
        for record in get_shard_records(streams_client, stream_arn, shard, 'TRIM_HORIZON'):
            pass # Get the final record
        sequence_number_bookmarks[shard['ShardId']] = record['dynamodb']['SequenceNumber']

    return sequence_number_bookmarks

def sync_log_based(config, state, stream):
    table_name = stream['tap_stream_id']

    client = dynamodb.get_client(config)
    streams_client = dynamodb.get_stream_client(config)

    md_map = metadata.to_map(stream['metadata'])
    projection = metadata.get(md_map, (), 'tap-mongodb.projection')
    
    # Write activate version message
    stream_version = singer.get_bookmark(state, table_name, 'version')
    singer.write_version(table_name, stream_version)

    table = dynamodb.describe_table(TableName=table_name)
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
            # TODO transform record with projection
            record_message = deserializer.deserialize_stream_record(record, projection)
            singer.write_record(table_name, record_message)
            rows_saved += 1





def has_stream_aged_out(config, state, stream):
    # TODO implement
    # get the min sequence number for each shard and compare to what we have bookmarked
    # if min seq num > bookmark, it has aged out

    # Also, if the 

    return false


