import time

import singer
from singer import metadata
from tap_dynamodb import dynamodb
from tap_dynamodb.deserialize import Deserializer

LOGGER = singer.get_logger()


def scan_table(table_name, projection, expression, last_evaluated_key, config):
    scan_params = {
        'TableName': table_name,
        'Limit': 1000
    }

    if projection is not None and projection != '':
        scan_params['ProjectionExpression'] = projection
    if expression:
        # Add `ExpressionAttributeNames` parameter for reserved word.
        scan_params['ExpressionAttributeNames'] = expression
    if last_evaluated_key is not None:
        scan_params['ExclusiveStartKey'] = last_evaluated_key

    client = dynamodb.get_client(config)
    has_more = True

    while has_more:
        LOGGER.info('Scanning table %s with params:', table_name)
        for key, value in scan_params.items():
            LOGGER.info('\t%s = %s', key, value)

        result = client.scan(**scan_params)
        yield result

        if result.get('LastEvaluatedKey'):
            scan_params['ExclusiveStartKey'] = result['LastEvaluatedKey']

        has_more = result.get('LastEvaluatedKey', False)


def sync(config, state, stream):
    table_name = stream['tap_stream_id']

    # before writing the table version to state, check if we had one to begin with
    first_run = singer.get_bookmark(state, table_name, 'version') is None

    # last run was interrupted if there is a last_id_fetched bookmark
    was_interrupted = singer.get_bookmark(state,
                                          table_name,
                                          'last_evaluated_key') is not None

    # pick a new table version if last run wasn't interrupted
    if was_interrupted:
        stream_version = singer.get_bookmark(state, table_name, 'version')
    else:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  table_name,
                                  'version',
                                  stream_version)
    singer.write_state(state)

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if first_run:
        singer.write_version(table_name, stream_version)

    last_evaluated_key = singer.get_bookmark(state,
                                             table_name,
                                             'last_evaluated_key')

    md_map = metadata.to_map(stream['metadata'])
    projection = metadata.get(md_map, (), 'tap-mongodb.projection')

    # An expression attribute name is a placeholder that one uses in an Amazon DynamoDB expression as an alternative to an actual attribute name.
    # Sometimes it might need to write an expression containing an attribute name that conflicts with a DynamoDB reserved word.
    # For example, table `A` contains the field `Comment` but `Comment` is a reserved word. So, it fails during fetch.
    expression = metadata.get(md_map, (), 'tap-mongodb.expression')

    if projection:
        # Split projection field value(fields that need to be fetched) to list
        projections = [x.strip() for x in projection.split(',')]
        if expression:
            # Split expression field value(reserved words) to list
            expressions = [x.strip() for x in expression.split(',')]
            projection, expression = prepare_expression(projections, expressions) # Prepare expression attributes for reserved word.
    rows_saved = 0

    deserializer = Deserializer()
    for result in scan_table(table_name, projection, expression, last_evaluated_key, config): # Pass extra expressions attribute for reserved word.
        for item in result.get('Items', []):
            rows_saved += 1
            # TODO: Do we actually have to put the item we retreive from
            # dynamo into a map before we can deserialize?
            record = deserializer.deserialize_item(item)
            record_message = singer.RecordMessage(stream=table_name,
                                                  record=record,
                                                  version=stream_version)

            singer.write_message(record_message)
        if result.get('LastEvaluatedKey'):
            state = singer.write_bookmark(state, table_name, 'last_evaluated_key', result.get('LastEvaluatedKey'))
            singer.write_state(state)

    state = singer.clear_bookmark(state, table_name, 'last_evaluated_key')

    state = singer.write_bookmark(state,
                                  table_name,
                                  'initial_full_table_complete',
                                  True)

    singer.write_state(state)

    singer.write_version(table_name, stream_version)

    return rows_saved

def get_expr_names(projection_element, expression_list):
    '''
    Prepare expression names for general expression attributes as well as for each of the nested
    expression attributes.
    For example:
    projections = ["Comment", "Ticket"], expressions = ["Comment"]
    return = "#Commnt"
    Example of nested expression :
    projections = ["Name[0].Comment"], expressions = ["Name.Comment"]
    return = "#Namme[0]" and "#Commnt" in the second loop call
    '''
    half_length = int(len(projection_element)/2)
    expr = "#{}".format(projection_element[:half_length+1]+projection_element[-2:]) # prepare expression attribute name
    # remove `[` and `]` from the expression name
    expr = expr.replace("[", "").replace("]", "")
    # if the projection is a list, it will contain the index in `[]`, hence we need to split
    # through '[' to get the index stored in the projection inside `[]` and append the remaining
    # to the newly created `expr`.
    proj_split = projection_element.split('[', 1)
    expression_list[expr] = proj_split[0]
    # if the length of the split is 1, it means there is no `[]` in the projection,
    # hence there's no need to append the index. If the length is 1, the second element
    # of the `proj_split` would be appended to the `expr`.
    if len(proj_split) != 1:
        expr = expr + '[' + proj_split[1]
    return expr

def prepare_expression(projections, expressions):
    """
    Prepare expression attributes for reserved word. Loop through all projections.
    If projection found in expressions(reserved word list) then prepare expression attribute name for the same with
    starting of # sign followed by the combination of 1st half part of projection, next character of 1st half and last 2 character of projection.
    Because as per the documentation an expression attribute name must begin with a pound sign (#), and be followed
    by one or more alphanumeric characters. Prepare expression Dict element with key as expression attribute name and
    value as projection and replace projection with expression attribute name in projections(list of fields that need
    to be fetched). Return expression dict and comma seprated string of projections.
    Example :
    projections = ["Comment", "Ticket"], expressions = ["Comment"]
    return = "#Commnt, Ticket", {"#Commnt" : "Comment"}
    Example of nested expression :
    projections = ["Name[0].Comment"], expressions = ["Name.Comment"]
    return = "#Namme[0].#Commnt", {"#Namme": "Name", "#Commnt": "Comment"}
    """
    i = 0
    expression_list = {}
    # Loop through all projection.
    for projection_element in projections:
        # Projection found in expressions(reserved word list)
        if projection_element in expressions:
            # if it is a nested projection, it needs to be handled differently
            if '.' in projection_element:
                # store the parent and th childs split by `.` in a list
                proj_list = projection_element.split('.')
                nested_expr = []
                for each_proj in proj_list:
                    expr = get_expr_names(each_proj, expression_list)
                    nested_expr.append(expr)
                # join the expression names by `.`
                expr = '.'.join(nested_expr)
                projections[i] = expr
            else:
                expr = get_expr_names(projection_element, expression_list)
                # Replace projection with expression attribute name in projections
                projections[i] = expr
        i = i + 1

    return ','.join(projections), expression_list
