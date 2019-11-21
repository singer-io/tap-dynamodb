import base64
from boto3.dynamodb.types import TypeDeserializer

class Deserializer(TypeDeserializer):
    '''
    This class inherits from boto3.dynamodb.types.TypeDeserializer
    https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/dynamodb/types.html
    By extending TypeDeserializer we get a lot of functionality for
    free (ie handling nested data) while allowing us to redefine how to
    handle binary data and sets
    '''
    def _deserialize_b(self, value):
        '''
        Deserializes binary data as a base64 encoded string because that's how
        the aws cli returns binary data
        '''
        return base64.b64encode(value).decode('utf-8')

    def _deserialize_ns(self, value):
        '''
        Deserializes sets as lists to allow JSON encoding
        '''
        return list(map(self._deserialize_n, value))

    def _deserialize_ss(self, value):
        '''
        Deserializes sets as lists to allow JSON encoding
        '''
        return list(map(self._deserialize_s, value))

    def _deserialize_bs(self, value):
        '''
        Deserializes sets as lists to allow JSON encoding
        '''
        return list(map(self._deserialize_b, value))
