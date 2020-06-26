import random

class TestDynamoDBBase(unittest.TestCase):
    def clear_tables(client, table_names):
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

    def create_table(client, table_name, hash_key, hash_type, sort_key, sort_type):
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
            }
        )
        print('Finished creating table: {}'.format(table_name))

    def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))


    def generate_simple_items_1(num_items):
        for i in range(num_items):
            yield {'int_id': { 'N': str(i) },
                'string_field': {'S': random_string_generator() } }


    def generate_simple_items_2(num_items):
        for i in range(num_items):
            yield {'string_id': { 'S': random_string_generator() },
                'int_field': {'N': str(i) } }


