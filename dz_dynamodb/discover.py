from singer import metadata
import singer
from datetime import datetime
import backoff
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError
from dz_dynamodb import dynamodb

LOGGER = singer.get_logger()


def discover_table_schema(client, table_name):
    '''
    For each of the tables in the dynamodb, create the table schema for the catalog
    '''
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
    db_schema = {
        'table_name': table_name,
        'stream': table_name,
        'tap_stream_id': table_name,
        'metadata': metadata.to_list(mdata),
        'schema': {
            'type': 'object',
             'properties': {
             }
        }
    }
    # Get columns and data types
    response = client.scan(TableName=table_name)
    items = response.get('Items', [])
    max_item_check = 1000
    for item in items:
        if(max_item_check>0):
            max_item_check-=1
            for column_name,column_value in item.items():
                val_type = map_dtype_of_value(column_value)
                if val_type == 'string' and is_date_time(column_value) :
                    db_schema['schema']['properties'][column_name] = {"type": val_type , "format":"date-time"}
                    continue
                db_schema['schema']['properties'][column_name] = {"type": val_type}
    return db_schema

def map_dtype_of_value(value):
    type_mapping = {
        'NULL': 'null',
        'S': 'string',
        'N': 'number',
        'BOOL': 'boolean',
        'L': 'array',
        'M': 'object'
    }
    if isinstance(value,dict):
        for key in value:
            if key in type_mapping:
                if callable(type_mapping[key]):
                    return type_mapping[key](value[key])
                return type_mapping[key]

    return 'unknown'

def is_date_time(datetime_str: str) -> bool:
    """
    Args:
        datetime_str: a string 
    
    Returns: True if datetime_str is string representation of datetime , False otherwise
    """
    # single item in dictionary
    datetime_str = next(iter(datetime_str.values()))
    datetime_formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f%z"
        ]

    for dt_fmt in datetime_formats:
        try:
            datetime.strptime(datetime_str, dt_fmt)
            return True
        except ValueError:
            pass 

    return False
# Backoff for both ReadTimeout and ConnectTimeout error for 5 times
@backoff.on_exception(backoff.expo,
                    (ReadTimeoutError, ConnectTimeoutError),
                    max_tries=5,
                    factor=2)
def discover_streams(config):
    '''
    Get the list of the tables and create the table schema for each of them
    '''
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
