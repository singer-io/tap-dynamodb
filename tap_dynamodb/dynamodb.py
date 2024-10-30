import json
import backoff
import boto3
import singer

from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    CredentialResolver,
    DeferredRefreshableCredentials,
    JSONFileCache,
    RefreshableCredentials
)
from botocore.session import Session
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError

LOGGER = singer.get_logger()
REQUEST_TIMEOUT = 300

def retry_pattern():
    '''
    Retry logic for the ClientError
    '''
    return backoff.on_exception(backoff.expo,
                                (ClientError, ConnectTimeoutError, ReadTimeoutError),
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    '''
    Log the retry attempt with the number of tries
    '''
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


class AssumeRoleProvider():
    METHOD = 'assume-role'

    def __init__(self, fetcher):
        self._fetcher = fetcher

    def load(self):
        return DeferredRefreshableCredentials(
            self._fetcher.fetch_credentials,
            self.METHOD
        )


@retry_pattern
def setup_aws_client(config):
    proxy_role_arn = "arn:aws:iam::{}:role/{}".format(config['proxy_account_id'].replace('-', ''),config['proxy_role_name'])
    cust_role_arn = "arn:aws:iam::{}:role/{}".format(config['account_id'].replace('-', ''),config['role_name'])

    # Step 1: Assume Role in Account Proxy and set up refreshable session
    session_proxy = Session()
    fetcher_proxy = AssumeRoleCredentialFetcher(
        client_creator=session_proxy.create_client,
        source_credentials=session_proxy.get_credentials(),
        role_arn=proxy_role_arn,
        extra_args={
            'DurationSeconds': 3600,
            'RoleSessionName': 'ProxySession',
            'ExternalId': config['proxy_external_id']
        },
        cache=JSONFileCache()
    )

    # Refreshable credentials for Account Proxy
    refreshable_credentials_proxy = RefreshableCredentials.create_from_metadata(
        metadata=fetcher_proxy.fetch_credentials(),
        refresh_using=fetcher_proxy.fetch_credentials,
        method="sts-assume-role"
    )

    # Step 2: Use Proxy Account's session to assume Role in Customer Account
    session_cust = Session()
    fetcher_cust = AssumeRoleCredentialFetcher(
        client_creator=session_cust.create_client,
        source_credentials=refreshable_credentials_proxy,
        role_arn=cust_role_arn,
        extra_args={
            'DurationSeconds': 3600,
            'RoleSessionName': 'CustSession',
            'ExternalId': config['external_id']
        },
        cache=JSONFileCache()
    )

    # # Refreshable credentials for Account Customer
    # refreshable_credentials_c = RefreshableCredentials.create_from_metadata(
    #     metadata=fetcher_cust.fetch_credentials(),
    #     refresh_using=fetcher_cust.fetch_credentials,
    #     method="sts-assume-role"
    # )

    # Set up refreshable session for Customer Account
    refreshable_session_cust = Session()
    refreshable_session_cust.register_component(
        'credential_provider',
        CredentialResolver([AssumeRoleProvider(fetcher_cust)])
    )

    LOGGER.info("Attempting to assume_role on RoleArn: %s", cust_role_arn)
    boto3.setup_default_session(botocore_session=refreshable_session_cust)

def get_request_timeout(config):
    # if request_timeout is other than 0,"0" or "" then use request_timeout
    request_timeout = config.get('request_timeout')
    if request_timeout and float(request_timeout):
        request_timeout = float(request_timeout)
    else: # If value is 0,"0" or "" then set default to 300 seconds.
        request_timeout = REQUEST_TIMEOUT
    return request_timeout

def get_client(config):
    """
    Client for FULL_TABLE and running discover mode.
    """
    # get the request_timeout
    request_timeout = get_request_timeout(config)
    # add the request_timeout in both connect_timeout as well as read_timeout
    timeout_config = Config(connect_timeout=request_timeout, read_timeout=request_timeout)
    if config.get('use_local_dynamo'):
        return boto3.client('dynamodb',
                            endpoint_url='http://localhost:8000',
                            region_name=config['region_name'],
                            config=timeout_config   # pass the config to add the request_timeout
                            )
    return boto3.client('dynamodb', config['region_name'],
                        config=timeout_config   # pass the config to add the request_timeout
                        )

def get_stream_client(config):
    """
    Streams client for the LOG_BASED sync.
    """
    # get the request_timeout
    request_timeout = get_request_timeout(config)
    # add the request_timeout in both connect_timeout as well as read_timeout
    timeout_config = Config(connect_timeout=request_timeout,  read_timeout=request_timeout)
    if config.get('use_local_dynamo'):
        return boto3.client('dynamodbstreams',
                            endpoint_url='http://localhost:8000',
                            region_name=config['region_name'],
                            config=timeout_config   # pass the config to add the request_timeout
                            )
    return boto3.client('dynamodbstreams',
                        region_name=config['region_name'],
                        config=timeout_config   # pass the config to add the request_timeout
                        )

def decode_expression(expression):
    '''Convert the string into JSON object and raise an exception if invalid JSON format'''
    try:
        return json.loads(expression)
    except json.decoder.JSONDecodeError:
        raise Exception("Invalid JSON format. The expression attributes should contain a valid JSON format.")
