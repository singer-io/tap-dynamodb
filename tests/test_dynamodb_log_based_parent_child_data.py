from boto3.dynamodb.types import TypeSerializer

from tap_tester import connections
from tap_tester import menagerie
from tap_tester import runner

from base import TestDynamoDBBase

class DynamoDBLogBasedParentChildData(TestDynamoDBBase):
    """
        Test case for verifying:
        - The tap does not break when the parent data is not found and the user is requesting for child data
        - The tap does not break when the data a specific position is not found in the record
    """

    # expected table configs
    def expected_table_config(self):
        return [
            {
                'TableName': 'simple_table_1',
                'HashKey': 'int_id',
                'HashType': 'N',
                'generator': self.generate_items,
                'num_rows': 10,
                'ProjectionExpression': 'int_id, map_field.map_entry_1, test_list_1[0], test_list_2[0], test_list_2[1], test_list_3[0].test_field',
                'top_level_keys': {'int_id', 'map_field'},
                'nested_map_keys': {'map_field': {'map_entry_1'}},
            }
        ]

    # send desired data for testing
    def generate_items(self, num_items, start_key=0):
        serializer = TypeSerializer()
        for i in range(start_key, start_key + num_items):
            record = {
                'int_id': i,
                'string_field': self.random_string_generator(),
                'test_list_2': ['list_2_data']
            }
            yield serializer.serialize(record)

    @staticmethod
    def name():
        return "tap_tester_dynamodb_parent_child_data"

    def test_run(self):
        (table_configs, conn_id, expected_streams) = self.pre_sync_test()

        # Select 'simple_table_1' stream and add replication method and projection
        found_catalogs = menagerie.get_catalogs(conn_id)
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [
                {
                    "breadcrumb": [],
                    "metadata": {
                        'replication-method': 'LOG_BASED',
                        'tap-mongodb.projection': table_configs[0]['ProjectionExpression']
                    }
                }
            ]
            connections.select_catalog_and_fields_via_metadata(conn_id,
                                                               stream_catalog,
                                                               annotated_schema,
                                                               additional_md)

        # diable stream to force shard to close
        self.disableStreams(expected_streams)
        # run sync mode 1st time as for the 1st time it sync in FULL_TABLE mode
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # collect state file
        state = menagerie.get_state(conn_id)
        state_version = menagerie.get_state_version(conn_id)

        # delete 'finished_shards' for every streams from the state file as we want to run 2nd sync
        for config in table_configs:
            table_name = config['TableName']
            del state['bookmarks'][table_name]['finished_shards']
        menagerie.set_state(conn_id, state, version=state_version)

        # run the sync mode 2nd time, noow it will run in LOG_BASED mode
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # get data
        messages_by_stream = runner.get_records_from_target_output()

        for stream in expected_streams:
            messages = messages_by_stream.get(stream).get('messages')
            records = [message.get('data') for message in messages if message.get('action') == 'upsert']
            for record in records:

                # verify that we get 'None' for child data when parent data is not found
                self.assertIsNone(record.get('map_field').get('map_entry_1'))
                # verify that we only get the available data if the data at a particular index is not found
                self.assertEquals(record.get('test_list_1'), [])
                self.assertEquals(record.get('test_list_2'), ['list_2_data'])
                # verify that we got empty map if the parent data at a particular index is not found for child data
                self.assertEquals(record.get('test_list_3'), [{}])
