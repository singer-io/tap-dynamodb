from boto3.dynamodb.types import TypeSerializer

from base import TestDynamoDBBase


class DynamoDBDiscovery(TestDynamoDBBase):
    def expected_table_config(self):
        return [
            {'TableName': 'simple_table_1',
             'HashKey': 'int_id',
             'HashType': 'N',
             'SortKey': 'string_field',
             'SortType': 'S',
             'generator': self.generate_simple_items_1,
             'num_rows': 50,
            },
            {'TableName': 'simple_table_2',
             'HashKey': 'string_id',
             'HashType': 'S',
             'SortKey': 'int_field',
             'generator': self.generate_simple_items_2,
             'num_rows': 50,
             'SortType': 'N'},
            {'TableName': 'simple_table_3',
             'HashKey': 'int_id',
             'generator': self.generate_simple_items_1,
             'num_rows': 50,
             'HashType': 'N'},
        ]

    def generate_simple_items_1(self, num_items):
        serializer = TypeSerializer()
        for i in range(num_items):
            record = {
                'int_id': i,
                'string_field': self.random_string_generator(),
            }
            yield serializer.serialize(record)

    def generate_simple_items_2(self, num_items):
        serializer = TypeSerializer()
        for i in range(num_items):
            record = {
                'string_id': self.random_string_generator(),
                'int_field': i,
            }
            yield serializer.serialize(record)

    @staticmethod
    def name():
        return "tap_tester_dynamodb_discovery"

    def test_run(self):
        self.pre_sync_test()
