import base64
import decimal
from boto3.dynamodb.types import TypeDeserializer

# Custom context to control how decimals are deserialized
# Precision = 100 because that's the max for `singer.decimal` elsewhere, still
# trapping Rounding errors because that is a true error.
trapped_signals = [decimal.Clamped, decimal.Overflow, decimal.Inexact, decimal.Rounded, decimal.Underflow]
SINGER_CONTEXT = decimal.Context(Emin=-128, Emax=126, prec=100,
                                 traps=trapped_signals)

class Deserializer(TypeDeserializer):
    '''
    This class inherits from boto3.dynamodb.types.TypeDeserializer
    https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/dynamodb/types.html
    By extending TypeDeserializer we get a lot of functionality for
    free (ie handling nested data) while allowing us to redefine how to
    handle binary data and sets
    '''

    def deserialize_item(self, item):
        return self.deserialize({'M': item})

    def _deserialize_b(self, value):
        '''
        Deserializes binary data as a base64 encoded string because that's how
        the aws cli returns binary data
        '''
        return base64.b64encode(value).decode('utf-8')

    def _deserialize_n(self, value):
        '''
        Deserializes sets as lists to allow JSON encoding
        '''
        return SINGER_CONTEXT.create_decimal(value)

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

    def _apply_projection(self, record, breadcrumb, output):
        """
        The LOG_BASED replication method uses the get_records method which gets all the records by default.
        In case of projection expression, parse the response retrieved from the API and filter the output
        based on projection expressions
        """
        if len(breadcrumb) == 1:
            if '[' in breadcrumb[0]:
                breadcrumb_key = breadcrumb[0].split('[')[0]
                index = int(breadcrumb[0].split('[')[1].split(']')[0])
                if output.get(breadcrumb_key):
                    # only prepare output if the list field contains data at that index position in record
                    if len(record.get(breadcrumb_key)) > index:
                        output[breadcrumb_key].append(record[breadcrumb_key][index])
                else:
                    output[breadcrumb_key] = []
                    # only prepare output if the list field contains data at that index position in record
                    if record.get(breadcrumb_key) and len(record.get(breadcrumb_key)) > index:
                        output[breadcrumb_key].append(record[breadcrumb_key][index])
            else:
                output[breadcrumb[0]] = record.get(breadcrumb[0])
        else:
            if '[' in breadcrumb[0]:
                breadcrumb_key = breadcrumb[0].split('[')[0]
                index = int(breadcrumb[0].split('[')[1].split(']')[0])
                if not output.get(breadcrumb_key):
                    output[breadcrumb_key] = [{}]
                # only prepare output if the list field contains data at that index position in record
                if record.get(breadcrumb_key) and len(record.get(breadcrumb_key)) > index:
                    self._apply_projection(record[breadcrumb_key][index], breadcrumb[1:], output[breadcrumb_key][0])
            else:
                if output.get(breadcrumb[0]) is None:
                    output[breadcrumb[0]] = {}
                # keep empty dict if the data is not found in the record
                if record.get(breadcrumb[0]):
                    self._apply_projection(record.get(breadcrumb[0], {}), breadcrumb[1:], output[breadcrumb[0]])

    def apply_projection(self, record, projections):
        output = {}

        for breadcrumb in projections:
            self._apply_projection(record, breadcrumb, output)

        return output
