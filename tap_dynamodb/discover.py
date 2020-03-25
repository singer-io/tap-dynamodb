from singer import metadata
import singer
from botocore.exceptions import ClientError
from tap_dynamodb import dynamodb

LOGGER = singer.get_logger()

def discover_table_schema(client, table_name):
    try:
        table_info = client.describe_table(TableName=table_name).get('Table', {})
    except ClientError:
        LOGGER.info('Access to table %s was denied, skipping', table_name)
        return None

    # write stream metadata
    mdata = {}
    key_props = [key_schema.get('AttributeName') for key_schema in table_info.get('KeySchema', [])]
    mdata = metadata.write(mdata, (), 'table-key-properties', key_props)
    if table_info.get('ItemCount'):
        mdata = metadata.write(mdata, (), 'row-count', table_info['ItemCount'])

    return {
        'table_name': table_name,
        'stream': table_name,
        'tap_stream_id': table_name,
        'metadata': metadata.to_list(mdata),
        'schema': {
            'type': 'object'
        }
    }


def discover_streams(config):
    client = dynamodb.get_client(config)

    try:
        response = client.list_tables()
    except ClientError:
        raise Exception("Authorization to AWS failed. Please ensure the role and policy are configured correctly on your AWS account.")

    table_list = response.get('TableNames')

    while response.get('LastEvaluatedTableName') is not None:
        response = client.list_tables(ExclusiveStartTableName=response.get('LastEvaluatedTableName'))
        table_list += response.get('TableNames')

    streams = [x for x in
               (discover_table_schema(client, table) for table in table_list)
               if x is not None]


    return streams
