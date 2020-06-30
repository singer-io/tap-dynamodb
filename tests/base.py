import random
import decimal
import unittest
import string

import boto3
from boto3.dynamodb.types import TypeSerializer

import singer

ALL_TABLE_NAMES_TO_CLEAR = {
    'simple_table_1',
    'simple_table_2',
    'simple_table_3',
    'com-stitchdata-test-dynamodb-integration-simple_table_1',
}
    
LOGGER = singer.get_logger()


class TestDynamoDBBase(unittest.TestCase):
    _client = None

    def setUp(self):
        client = self.dynamodb_client()

        table_configs = self.expected_table_config()

        self.clear_tables(client)

        for table in table_configs:
            self.create_table(client,
                              table['TableName'],
                              table['HashKey'],
                              table['HashType'],
                              table.get('SortKey'),
                              table.get('SortType'))

        waiter = client.get_waiter('table_exists')
        for table in table_configs:
            LOGGER.info('Adding Items for {}'.format(table['TableName']))
            waiter.wait(TableName=table['TableName'], WaiterConfig={"Delay": 1, "MaxAttempts": 20})
            for item in table['generator'](table['num_rows']):
                client.put_item(TableName=table['TableName'], Item=item['M'])

    def tearDown(self):
        self.clear_tables(self.dynamodb_client())
    
    def dynamodb_client(self):
        if not self._client:
            self._client = boto3.client('dynamodb',
                                   endpoint_url='http://localhost:8000',
                                   region_name='us-east-1')
        return self._client
    
    def create_table(self, client, table_name, hash_key, hash_type, sort_key, sort_type):
        self.assertIn(table_name, ALL_TABLE_NAMES_TO_CLEAR)
        print('\nCreating table: {}'.format(table_name))

        key_schema = [
            {
                'AttributeName': hash_key,
                'KeyType': 'HASH'  #Partition key
            },
        ]

        attribute_defs = [
            {
                'AttributeName': hash_key,
                'AttributeType': hash_type
            },
        ]

        if sort_key:
            key_schema.append({ 'AttributeName': sort_key, 'KeyType': 'RANGE' })
            attribute_defs.append({ 'AttributeName': sort_key, 'AttributeType': sort_type })

        client.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_defs,
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            },
            StreamSpecification={
                'StreamEnabled': True,
                'StreamViewType': 'NEW_IMAGE'
            },
        )
        print('Finished creating table: {}'.format(table_name))

    def clear_tables(self, client, table_names=ALL_TABLE_NAMES_TO_CLEAR):
        existing_table_names = client.list_tables()['TableNames']
        existing_table_names_to_clear = table_names.intersection(set(existing_table_names))
        for table_name in existing_table_names_to_clear:
            try:
                table = client.delete_table(TableName=table_name)
            except Exception as e:
                print('\nCould not clear table {} due to error {}'.format(table_name, e))

        # wait for all tables to be deleted
        waiter = client.get_waiter('table_not_exists')
        for table_name in existing_table_names_to_clear:
            try:
                waiter.wait(TableName=table_name, WaiterConfig={"Delay": 3, "MaxAttempts": 20})        
            except Exception as e:
                print('\nCould not wait on table {} due to error {}'.format(table_name, e))

    def random_string_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))

    def generate_items(self, num_items):
        serializer = TypeSerializer()
        for i in range(num_items):
            record = {
                'int_id': i,
                'decimal_field': decimal.Decimal(str(i) + '.00000000001'),
                'string_field': self.random_string_generator(),
                'byte_field': b'some_bytes',
                'int_list_field': [i, i+1, i+2],
                'int_set_field': set([i, i+1, i+2]),
                'map_field': {
                    'map_entry_1': 'map_value_1',
                    'map_entry_2': 'map_value_2'
                },
                'string_list': [self.random_string_generator(), self.random_string_generator(), self.random_string_generator()],
                'boolean_field': True,
                'other_boolean_field': False,
                'null_field': None
            }
            yield serializer.serialize(record)

    def enableStreams(self, table_names):
        client = self.dynamodb_client()

        for table_name in table_names:
            client.update_table(
                TableName=table_name,
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_IMAGE'
                },
            )

    def disableStreams(self, table_names):
        client = self.dynamodb_client()

        for table_name in table_names:
            client.update_table(
                TableName=table_name,
                StreamSpecification={
                    'StreamEnabled': False,
                },
            )

    def addMoreData(self, numRows):
        client = self.dynamodb_client()

        table_configs = self.expected_table_config()

        for table in table_configs:
            LOGGER.info('Adding {} Items for {}'.format(numRows, table['TableName']))
            for item in table['generator'](numRows, table['num_rows']):
                client.put_item(TableName=table['TableName'], Item=item['M'])

    def updateData(self, numRows):
        client = self.dynamodb_client()

        table_configs = self.expected_table_config()

        newId = 200
        for table in table_configs:
            LOGGER.info('Updating {} Items for {}'.format(numRows, table['TableName']))
            for item in table['generator'](numRows):
                client.update_item(TableName=table['TableName'],
                                   Key=item['M'],
                                   UpdateExpression='SET int_id = :newId',
                                   ExpressionAttributeValues={
                                       ':newId': {'N': str(newId)},
                                   },
                )
                newId = newId + 1

    def deleteData(self, id_range):
        client = self.dynamodb_client()

        for table in self.expected_table_config():
            LOGGER.info('Deleting {} Items for {}'.format(len(id_range), table['TableName']))
            for id in id_range:
                client.delete_item(TableName=table['TableName'],
                                   Key={'int_id': {
                                       'N': str(id)}})

