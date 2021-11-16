from tap_dynamodb.sync_strategies import full_table, log_based
from tap_dynamodb import discover, dynamodb
import tap_dynamodb
import unittest
from unittest import mock
from unittest.mock import Mock
from unittest.case import TestCase
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError

class MockClient():
    def __init__(self, endpoint_url=None):
        pass

    def list_tables(self):
        pass



class TestBackoffError(unittest.TestCase):
    '''
    Test that backoff logic works properly.
    '''
    # @mock.patch('tap_dynamodb.dynamodb.boto3.client', return_value=MockClient())
    @mock.patch('boto3.client')
    @mock.patch('singer.metadata.to_map')
    def test_discover_stream_request_timeout_and_backoff(self, mock_map, mock_client):
        """
        Check whether the request backoffs properly for list_tables for 5 times in case of Timeout error.
        """
        mock_client.list_tables.side_effect = ReadTimeoutError
        # mock_client = Mock()
        # mock_client.list_tables = Mock()

        with self.assertRaises(ReadTimeoutError):
            # log_based.sync({"region_name": "us-east-2"}, {}, {"tap_stream_id": "dummy", "metadata":""})
            discover.discover_streams({"region_name": "dummy", "use_local_dynamo": "true"})
        self.assertEquals(mock_client.list_tables.call_count, 5)


class TestRequestTimeoutValue(unittest.TestCase):
    '''
    Test that request timeout parameter works properly in various cases
    '''
    @mock.patch('boto3.client')
    @mock.patch("tap_dynamodb.dynamodb.Config")
    def test_config_provided_request_timeout(self, mock_config, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on config value
        """
        config = {"region_name": "dummy_region", "request_timeout": 100}
        dynamodb.get_client(config)
        mock_config.assert_called_with(connect_timeout=100, read_timeout=100)

    @mock.patch('boto3.client')
    @mock.patch("tap_dynamodb.dynamodb.Config")
    def test_default_value_request_timeout(self, mock_config, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based default value
        """
        config = {"region_name": "dummy_region"}
        dynamodb.get_client(config)
        mock_config.assert_called_with(connect_timeout=300, read_timeout=300)

    @mock.patch('boto3.client')
    @mock.patch("tap_dynamodb.dynamodb.Config")
    def test_config_provided_empty_request_timeout(self, mock_config, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on default value if empty value is given in config
        """
        config = {"region_name": "dummy_region", "request_timeout": ""}
        dynamodb.get_client(config)
        mock_config.assert_called_with(connect_timeout=300, read_timeout=300)

    @mock.patch('boto3.client')
    @mock.patch("tap_dynamodb.dynamodb.Config")
    def test_config_provided_string_request_timeout(self, mock_config, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on config string value
        """
        config = {"region_name": "dummy_region", "request_timeout": "100"}
        dynamodb.get_client(config)
        mock_config.assert_called_with(connect_timeout=100, read_timeout=100)

    @mock.patch('boto3.client')
    @mock.patch("tap_dynamodb.dynamodb.Config")
    def test_config_provided_float_request_timeout(self, mock_config, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on config float value
        """
        config = {"region_name": "dummy_region", "request_timeout": 100.8}
        dynamodb.get_client(config)
        mock_config.assert_called_with(connect_timeout=100.8, read_timeout=100.8)

    @mock.patch('boto3.client')
    @mock.patch("tap_dynamodb.dynamodb.Config")
    def test_stream_config_provided_request_timeout(self, mock_config, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on config value
        """
        config = {"region_name": "dummy_region", "request_timeout": 100}
        dynamodb.get_stream_client(config)
        mock_config.assert_called_with(connect_timeout=100, read_timeout=100)

    @mock.patch('boto3.client')
    @mock.patch("tap_dynamodb.dynamodb.Config")
    def test_stream_default_value_request_timeout(self, mock_config, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based default value
        """
        config = {"region_name": "dummy_region"}
        dynamodb.get_stream_client(config)
        mock_config.assert_called_with(connect_timeout=300, read_timeout=300)

    @mock.patch('boto3.client')
    @mock.patch("tap_dynamodb.dynamodb.Config")
    def test_stream_config_provided_empty_request_timeout(self, mock_config, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on default value if empty value is given in config
        """
        config = {"region_name": "dummy_region", "request_timeout": ""}
        dynamodb.get_stream_client(config)
        mock_config.assert_called_with(connect_timeout=300, read_timeout=300)

    @mock.patch('boto3.client')
    @mock.patch("tap_dynamodb.dynamodb.Config")
    def test_stream_config_provided_string_request_timeout(self, mock_config, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on config string value
        """
        config = {"region_name": "dummy_region", "request_timeout": "100"}
        dynamodb.get_stream_client(config)
        mock_config.assert_called_with(connect_timeout=100, read_timeout=100)

    @mock.patch('boto3.client')
    @mock.patch("tap_dynamodb.dynamodb.Config")
    def test_stream_config_provided_float_request_timeout(self, mock_config, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on config float value
        """
        config = {"region_name": "dummy_region", "request_timeout": 100.8}
        dynamodb.get_stream_client(config)
        mock_config.assert_called_with(connect_timeout=100.8, read_timeout=100.8)