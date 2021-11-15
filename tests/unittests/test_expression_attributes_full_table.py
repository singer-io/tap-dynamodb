import unittest
from unittest.mock import patch
from tap_dynamodb.sync_strategies.full_table import sync, scan_table

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
        return kwargs

@patch('singer.metadata.to_map', return_value = {})
@patch('singer.write_state', return_value = {})
@patch('singer.write_bookmark', return_value = {})
@patch('singer.get_bookmark', return_value = {})
class TestExpressionAttributesInFullTable(unittest.TestCase):
    """Test expression attributes for reserved word in full table sync. Mocked some method of singer package"""

    @patch('singer.metadata.get', side_effect = ["Comment, Sheet", "Comment"])
    @patch('tap_dynamodb.sync_strategies.full_table.scan_table', return_value = {}) 
    def test_sync_with_single_expression(self, mock_scan_table, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for single reserve word passed in `expression` field."""
        res = sync(CONFIG, STATE, STREAM)

        # Verify that `scan_table` called with expected paramater including expression attributes
        mock_scan_table.assert_called_with('GoogleDocs', '#Comt,Sheet', {'#Comt': 'Comment'}, {}, {'start_date': '2017-01-01', 
        'account_id': 'dummy_account_id', 'role_name': 'dummy_role', 'external_id': 'dummy_external_id', 'region_name': 'dummy_region_name'})


    @patch('tap_dynamodb.dynamodb.get_client', return_value = MockClient())        
    def test_scan_table_with_single_expression(self, mock_get_client, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test that scan method called with extra `ExpressionAttributeNames` parameter for single reserve word. 
        Here we mocked get_client with return value as object of MockClient class. Mcked scan method to return arguments 
        which we passed."""
        res = list(scan_table('', '#Comt,Sheet', {'#Comt': 'Comment'}, {}, {}))
        self.assertEqual(res, [{'ExclusiveStartKey': {},'ExpressionAttributeNames': {'#Comt': 'Comment'},'Limit': 1000,
                                'ProjectionExpression': '#Comt,Sheet','TableName': ''}])

    @patch('singer.metadata.get', side_effect = ["Comment, Sheet", "Comment, Sheet"])
    @patch('tap_dynamodb.sync_strategies.full_table.scan_table', return_value = {}) 
    def test_sync_with_multiple_expression(self, mock_scan_table, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for multiple reserve word passed in `expression` field."""
        res = sync(CONFIG, STATE, STREAM)

        # Verify that `scan_table` called with expected paramater including multiple expression attributes
        mock_scan_table.assert_called_with('GoogleDocs', '#Comt,#Sht', {'#Comt': 'Comment', '#Sht': 'Sheet'}, {}, {'start_date': '2017-01-01', 
        'account_id': 'dummy_account_id', 'role_name': 'dummy_role', 'external_id': 'dummy_external_id', 'region_name': 'dummy_region_name'})


    @patch('tap_dynamodb.dynamodb.get_client', return_value = MockClient())        
    def test_scan_table_with_multiple_expression(self, mock_get_client, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test that scan method called with extra `ExpressionAttributeNames` parameter for multiple reserve word. 
        Here we mocked get_client with return value as object of MockClient class. Mcked scan method to return arguments 
        which we passed."""
        res = list(scan_table('', '#Comt,#Sht', {'#Comt': 'Comment', '#Sht': 'Sheet'}, {}, {}))
        self.assertEqual(res, [{'ExclusiveStartKey': {},'ExpressionAttributeNames': {'#Comt': 'Comment', '#Sht': 'Sheet'},
                                'Limit': 1000, 'ProjectionExpression': '#Comt,#Sht','TableName': ''}])
        
    @patch('singer.metadata.get', side_effect = ["Comment, Sheet", ""])
    @patch('tap_dynamodb.sync_strategies.full_table.scan_table', return_value = {}) 
    def test_sync_without_expression(self, mock_scan_table, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for no reserve word passed in `expression` field. """
        res = sync(CONFIG, STATE, STREAM)

        mock_scan_table.assert_called_with('GoogleDocs', 'Comment,Sheet', {}, {}, {'start_date': '2017-01-01', 
        'account_id': 'dummy_account_id', 'role_name': 'dummy_role', 'external_id': 'dummy_external_id', 'region_name': 'dummy_region_name'})


    @patch('tap_dynamodb.dynamodb.get_client', return_value = MockClient())        
    def test_scan_table_without_expression(self, mock_get_client, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test that scan method does not called with extra `ExpressionAttributeNames` parameter when no reserve word passed 
        Here we mocked get_client with return value as object of MockClient class. Mcked scan method to return arguments 
        which we passed."""
        res = list(scan_table('', 'Comment,Sheet', {}, {}, {}))
        
        # Verify that `scan_table` called with expected paramater including empty expression attributes
        self.assertEqual(res, [{'ExclusiveStartKey': {},'Limit': 1000, 'ProjectionExpression': 'Comment,Sheet','TableName': ''}])