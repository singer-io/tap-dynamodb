import singer

from boto3.dynamodb.types import TypeSerializer

from tap_tester import connections
from tap_tester import menagerie
from tap_tester import runner

from base import TestDynamoDBBase

LOGGER = singer.get_logger()


class DynamoDBLogBasedPrimaryAndHashKeyReservedWords(TestDynamoDBBase):
    def expected_table_config(self):
        return [
            {
                'TableName': 'simple_table_4',
                # Added the `Comment` which is a reserved word as the primary key (HashKey) to verify the expression attributes works for them
                'HashKey': 'Comment',
                'HashType': 'N',
                # Added the `Name` which is a reserved word as the replication key (SortKey) to verify the expression attributes works for them
                'SortKey': 'Name',
                'SortType': 'S',
                'generator': self.generate_simple_items_4,
                'num_rows': 100,
                'ProjectionExpression': '#cmt, #name',
                'top_level_keys': {'Name', 'Comment'}
            }
        ]

    @staticmethod
    def generate_simple_items_4(num_items, start_key=0):
        '''Generate unique records for the table.'''
        serializer = TypeSerializer()
        for i in range(start_key, start_key + num_items):
            record = {
                'Comment': i,
                'Name': 'Test Name' + str(i)
            }
            yield serializer.serialize(record)

    @staticmethod
    def name():
        return "tt_dynamodb_log_pkhk_projections"

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
                        'tap-dz-dynamodb.expression-attributes': "{\"#cmt\": \"Comment\", \"#name\": \"Name\"}", # `expression` field for reserve word.
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
                        # Verify that the reserved words as primary keys and replication keys are replicated.
                        self.assertEqual(config['top_level_keys'], top_level_keys)
