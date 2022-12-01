import decimal
import singer

from boto3.dynamodb.types import TypeSerializer

from tap_tester import connections
from tap_tester import menagerie
from tap_tester import runner

from base import TestDynamoDBBase

LOGGER = singer.get_logger()


class DynamoDBLogBasedProjections(TestDynamoDBBase):
    def expected_table_config(self):
        return [
            {
                'TableName': 'simple_table_1',
                'HashKey': 'int_id',
                'HashType': 'N',
                'generator': self.generate_items,
                'num_rows': 100,
                # Added extra reserve words to verify the sync to retrieve `Absolute`, `Comment` and `Name[1].Comment`
                # and test_object.nested_field as a field and test_object.nested_field as a nested field.
                'ProjectionExpression': '#name[1].#cmt, #name[2].#testfield.#cmt, #abs, #cmt, #tstobj.#nestf, #tobj_nested, int_id, string_field, decimal_field, int_list_field[1], map_field.map_entry_1, string_list[2], map_field.list_entry[2], list_map[1].a',
                'top_level_keys': {'Name', 'Absolute', 'Comment', 'int_id', 'string_field', 'test_object', 'test_object.nested_field', 'decimal_field', 'int_list_field', 'map_field', 'string_list', 'list_map'},
                'top_level_list_keys': {'int_list_field', 'string_list', 'list_map', 'Name'},
                'nested_map_keys': {'map_field': {'map_entry_1', 'list_entry'}},
            }
        ]

    def generate_items(self, num_items, start_key=0):
        serializer = TypeSerializer()
        for i in range(start_key, start_key + num_items):
            record = {
                'Comment': 'Talend stitch',
                'Name': ['name1', {'Comment': "Test_comment"}, {"TestField": {"Comment": "For test"}}],
                'test_object': {"nested_field": "nested test value"},
                'test_object.nested_field': "test value with special character", # added this field to verify the `.` in the field names works properly.
                'Absolute': 'true',
                'int_id': i,
                'decimal_field': decimal.Decimal(str(i) + '.00000000001'),
                'string_field': self.random_string_generator(),
                'byte_field': b'some_bytes',
                'int_list_field': [i, i + 1, i + 2],
                'int_set_field': set([i, i + 1, i + 2]),
                'map_field': {
                    'map_entry_1': 'map_value_1',
                    'map_entry_2': 'map_value_2',
                    'list_entry': [i, i + 1, i + 2]
                },
                'list_map': [
                    {'a': 1,
                     'b': 2},
                    {'a': 100,
                     'b': 200}
                ],
                'string_list': [self.random_string_generator(), self.random_string_generator(), self.random_string_generator()],
                'boolean_field': True,
                'other_boolean_field': False,
                'null_field': None
            }
            yield serializer.serialize(record)

    @staticmethod
    def name():
        return "tt_dynamodb_log_projections"

    def test_run(self):
        (table_configs, conn_id, expected_streams) = self.pre_sync_test()

        # Select simple_coll_1 and simple_coll_2 streams and add replication method metadata
        found_catalogs = menagerie.get_catalogs(conn_id)
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [
                {
                    "breadcrumb": [],
                    "metadata": {
                        'replication-method': 'LOG_BASED',
                        'tap-dz-dynamodb.expression-attributes': "{\"#cmt\": \"Comment\", \"#testfield\": \"TestField\", \"#name\": \"Name\", \"#abs\": \"Absolute\", \"#tstobj\": \"test_object\", \"#nestf\": \"nested_field\", \"#tobj_nested\": \"test_object.nested_field\"}", # `expression` field for reserve word.
                        'tap-mongodb.projection': table_configs[0]['ProjectionExpression']
                    }
                }
            ]
            connections.select_catalog_and_fields_via_metadata(conn_id,
                                                               stream_catalog,
                                                               annotated_schema,
                                                               additional_md)

        self.first_sync_test(table_configs, conn_id, expected_streams)

        ################################
        # Run sync SECOND TIME and check that no records came through
        ################################
        # Disable streams forces shards to close
        self.disableStreams(expected_streams)
        sync_job_name = runner.run_sync_mode(self, conn_id)
        self.enableStreams(expected_streams)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()

        # Check that we only have 1 message (activate_version) on syncing
        # a stream without changes
        for stream in messages_by_stream.values():
            self.assertEqual(1, len(stream['messages']))

        menagerie.get_state(conn_id)

        # Add 10 rows to the DB
        self.addMoreData(10)
        # Delete some rows
        self.deleteData(range(40, 50))
        # Change some rows
        self.updateData(10, 60, 'boolean_field', False)

        ################################
        # Run sync THIRD TIME and check that records did come through
        ################################
        # Disable streams forces shards to close
        self.disableStreams(expected_streams)
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()

        for config in table_configs:
            table_name = config['TableName']

            for message in messages_by_stream[table_name]['messages']:
                if message['action'] == 'upsert':
                    if not message['data'].get('_sdc_deleted_at'):
                        top_level_keys = {*message['data'].keys()}
                        self.assertEqual(config['top_level_keys'], top_level_keys)
                        for list_key in config['top_level_list_keys']:
                            self.assertTrue(isinstance(message['data'][list_key], list))
                        self.assertEqual(config['nested_map_keys']['map_field'], {*message['data']['map_field'].keys()})

        # Check that we have 31 messages come through (10 upserts, 10 deletes, 10 updated records and 1 activate version)
        for stream in messages_by_stream.values():
            self.assertEqual(31, len(stream['messages']))

        menagerie.get_state(conn_id)

    def first_sync_test(self, table_configs, conn_id, expected_streams):
        ###################################
        # SYNC MODE FIRST RUN
        ###################################
        # Disable streams forces shards to close
        self.disableStreams(expected_streams)
        sync_job_name = runner.run_sync_mode(self, conn_id)
        self.enableStreams(expected_streams)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()
        expected_pks = {}

        for config in table_configs:
            key = {config['HashKey']}
            if config.get('SortKey'):
                key |= {config.get('SortKey')}
            expected_pks[config['TableName']] = key

        # assert that each of the streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   {x['TableName'] for x in table_configs},
                                                                   expected_pks)

        state = menagerie.get_state(conn_id)

        first_versions = {}

        # assert that we get the correct number of records for each stream
        for config in table_configs:
            table_name = config['TableName']

            self.assertEqual(config['num_rows'],
                             record_count_by_stream[table_name])

            # assert that an activate_version_message is first and last message sent for each stream
            self.assertEqual('activate_version',
                             messages_by_stream[table_name]['messages'][0]['action'])
            self.assertEqual('activate_version',
                             messages_by_stream[table_name]['messages'][-1]['action'])

            # assert that the state has an initial_full_table_complete == True
            self.assertTrue(state['bookmarks'][table_name]['initial_full_table_complete'])
            # assert that there is a version bookmark in state
            first_versions[table_name] = state['bookmarks'][table_name]['version']
            self.assertIsNotNone(first_versions[table_name])

        for config in table_configs:
            table_name = config['TableName']

            for message in messages_by_stream[table_name]['messages']:
                if message['action'] == 'upsert':
                    if not message['data'].get('_sdc_deleted_at'):
                        top_level_keys = {*message['data'].keys()}
                        self.assertEqual(config['top_level_keys'], top_level_keys)
                        for list_key in config['top_level_list_keys']:
                            self.assertTrue(isinstance(message['data'][list_key], list))
                        self.assertEqual(config['nested_map_keys']['map_field'], {*message['data']['map_field'].keys()})
