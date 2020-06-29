import random
import decimal
import unittest
import string

from boto3.dynamodb.types import TypeSerializer

ALL_TABLE_NAMES_TO_CLEAR = {
    'simple_table_1',
    'simple_table_2',
    'simple_table_3',
    'com-stitchdata-test-dynamodb-integration-simple_table_1',
}
    

class TestDynamoDBBase(unittest.TestCase):
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
        for table_name in table_names:
            try:
                table = client.delete_table(TableName=table_name)
            except Exception as e:
                print('\nCould not clear table {} due to error {}'.format(table_name, e))

        # wait for all tables to be deleted
        waiter = client.get_waiter('table_not_exists')
        for table_name in table_names:
            try:
                waiter.wait(TableName=table_name, WaiterConfig={"Delay": 3, "MaxAttempts": 20})        
            except Exception as e:
                print('\nCould not wait on table {} due to error {}'.format(table_name, e))

        list_tables_output = client.list_tables()['TableNames']
        print('\nlist_tables output after clearing tables:{}'.format(list_tables_output))

    def random_string_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))

    def generate_items(self, num_items):
        serializer = TypeSerializer()
        for i in range(num_items):
            record = {
                'int_id': i,
                'decimal_field': decimal.Decimal(str(i) + '.00000000001'),
                'string_field': random_string_generator(),
                'byte_field': b'some_bytes',
                'int_list_field': [i, i+1, i+2],
                'int_set_field': set([i, i+1, i+2]),
                'map_field': {
                    'map_entry_1': 'map_value_1',
                    'map_entry_2': 'map_value_2'
                },
                'string_list': [random_string_generator(), random_string_generator(), random_string_generator()],
                'boolean_field': True,
                'other_boolean_field': False,
                'null_field': None
            }
            yield serializer.serialize(record)


