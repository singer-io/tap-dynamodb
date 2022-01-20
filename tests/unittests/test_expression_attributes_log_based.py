import unittest
from unittest.mock import patch
from tap_dynamodb.sync_strategies.log_based import sync, prepare_projection
from tap_dynamodb.sync import sync_stream

CONFIG = {
    "start_date": "2017-01-01",
    "account_id": "dummy_account_id",
    "role_name": "dummy_role",
    "external_id": "dummy_external_id",
    "region_name": "dummy_region_name"
}
STATE = {}
STREAM = {
    "table_name": "GoogleDocs",
    "stream": "GoogleDocs",
    "tap_stream_id": "GoogleDocs",
    "metadata": []
}
class MockClient():
    def scan(self, **kwargs):
        '''Mock the scan() function of the client.'''
        return kwargs

    def describe_table(self, **kwargs):
        '''Mock the describe_table() function of the client.'''
        return {'Table': {'LatestStreamArn': 'dummy_arn'}}

    def describe_stream(self, **kwargs):
        '''Mock the describe stream function of the client.'''
        return {'StreamDescription': {'Shards': [{'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}]}}

    def get_shard_iterator(self, **kwargs):
        '''Mock the get_shard_iterator() of the client.'''
        return {'ShardIterator': {}}

class MockDeserializer():
    def __init__(self):
        return {}

client = MockClient()
@patch('singer.metadata.to_map', return_value = {})
@patch('singer.write_state', return_value = {})
@patch('singer.write_bookmark', return_value = {})
@patch('singer.get_bookmark', return_value = {})
class TestExpressionAttributesInLogBasedSync(unittest.TestCase):
    """Test expression attributes for reserved word in log_based sync. Mocked some method of singer package"""

    @patch('singer.metadata.get', side_effect = ["#c, Sheet", "{\"#c\": \"Comment\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_with_single_expression(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for single reserve word passed in `expression` field."""
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['Comment'], ['Sheet']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect = ["#tst[4], #n, Test", "{\"#tst\": \"test1\", \"#n\": \"Name\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_with_multiple_expression(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for multiple reserve words passed in `expression` field."""
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['test1[4]'], ['Name'], ['Test']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["Comment, Sheet", ""])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_without_expression(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute with empty string passed in `expression` field."""   
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['Comment'], ['Sheet']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["", ""])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_without_projection(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute with empty string passed in `projection` field."""  
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["#tst[4].#n, #tst[4].#a, Test", "{\"#tst\": \"test1\", \"#n\": \"Name\", \"#a\": \"Age\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_with_nested_expr_with_dict_and_list(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for nested reserved words with dictionary and list passed in `expression` field."""
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['test1[4]', 'Name'], ['test1[4]', 'Age'], ['Test']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["#tst[4], Test", "{\"#tst\": \"test1\", \"#n\": \"Name\", \"#a\": \"Age\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_with_nested_expr_with_list(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for reserve words with list passed in `expression` field."""
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['test1[4]'], ['Test']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["#tst.#n.#a", "{\"#tst\": \"test1\", \"#n\": \"Name\", \"#a\": \"Age\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_with_nested_expr_with_nested_dict(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for nested reserved words with nested dictionary passed in `expression` field."""
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['test1', 'Name', 'Age']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["#tst.#f, #tf", "{\"#tst\": \"test1\", \"#f\": \"field\", \"#tf\": \"test1.field\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_with_special_character_in_field_name(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for `.` in projection field passed in `expression` field."""
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['test1', 'field'], ['test1.field']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["#test, #t[1].#n", "{\"#t\": \"test1\", \"#n\": \"Name\", \"#test\": \"test\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_for_different_order_in_projections(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for `.` in projection field passed in `expression` field."""
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['test'], ['test1[1]', 'Name']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["#cmt", "{\"#cmt\": \"Comment\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.prepare_projection', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    def test_prepare_projections_called_when_expressions_exist(self, mock_stream_client, mock_client, mock_prepare_projection, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test that the prepare_projection() is called with the correct values when expression is passed in the catalog."""
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        mock_prepare_projection.assert_called_with(['#cmt'], '{"#cmt": "Comment"}')

    @patch('singer.metadata.get', side_effect =["#cmt", ""])
    @patch('tap_dynamodb.sync_strategies.log_based.prepare_projection', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    def test_prepare_projections_called_when_null_expressions(self, mock_stream_client, mock_client, mock_prepare_projection, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test that the prepare_projection() is not called when expression attributes are not passed in the catalog."""
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        self.assertEqual(mock_prepare_projection.call_count, 0)

    @patch('singer.metadata.get', side_effect = ["#c, Sheet", "{\"#c\":, \"Comment\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_stream_with_invalid_json(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test sync_stream raises exception with proper error message with invalid json in expression."""
        mock_client.return_value = client
        mock_stream_client.return_value = client
        try:
            res = sync_stream(CONFIG, STATE, STREAM)
        except Exception as e:
            expected_error_message = "Invalid JSON format. The expression attributes should contain a valid JSON format."
            self.assertEqual(str(e), expected_error_message)

class TestPrepareProjection(unittest.TestCase):
    def test_prepare_projection_output(self):
        """"""
        projection = ["#tst", "#cmt"]
        expression = "{\"#tst\": \"test\", \"#cmt\": \"Comment\"}"
        new_projection = prepare_projection(projection, expression)
        expected_projection = ["test", "Comment"]
        self.assertEqual(new_projection, expected_projection)