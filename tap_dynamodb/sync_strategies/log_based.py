import boto3
from tap_dynamodb import client

def get_latest_seq_number(config):
    stream_arn = table.latest_stream_arn
    dynamodb = client.get_stream_client(config)

    selected_tables = set(['simple_table'])
    stream_list = client.list_streams()
    for streamarn in (x['StreamArn'] for x in stream_list['Streams'] if x['TableName'] in selected_tables):
        stream_info = client.describe_stream(StreamArn=streamarn)
        last_shard = stream_info['StreamDescription']['Shards'][-1]
        shard_iterator = client.get_shard_iterator(StreamArn = streamarn,
                                                   ShardId = last_shard['ShardId'],
                                                   ShardIteratorType = 'TRIM_HORIZON')


        records = handle_shard_iterator(client, shard_iterator['ShardIterator'])

def sync_log_based(config, state, stream):
    dynamodb = client.get_stream_client(config)

    pass
