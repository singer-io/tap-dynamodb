# tap-dz-dynamodb

[![CircleCI](https://circleci.com/gh/singer-io/tap-dz-dynamodb.svg?style=svg)](https://circleci.com/gh/singer-io/tap-dz-dynamodb)

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## Configuration

This tap can get it's credentials in a variety of ways, including [environment variables](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#environment-variables), [shared credentials file](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#shared-credentials-file), [AWS config file](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#aws-config-file), [assume role provider](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#assume-role-provider), and [IAM roles](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#iam-roles).

The config file requires `region_name`. You can optionally add `account_id`, `external_id`, and `role_name` to have this tap assume that AWS Role. For testing you can specify `use_local_dynamo` to run against a local instance of [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html).

Copyright &copy; 2019 Stitch
