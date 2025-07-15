from decimal import Decimal
from boto3.dynamodb.types import TypeDeserializer

# from CompilerStructuresModule.CompilerStructures.frequencyCompiler import FrequencyCompiler
# from CompilerStructuresModule.CompilerStructures.gameAttributeTrie import GameAttributeTrie

deserializer = TypeDeserializer()

def prepareItem(game):
    item = {}
    for key, value in game.items():
        item[key] = convertToDynamodbFormat(value)
    return item
def convertToDynamodbFormat(value):
    if isinstance(value, str):
        return {"S": value}
    elif isinstance(value, bytes):
        return {"B": value}
    elif isinstance(value, bool):
        return {"BOOL": value}
    elif isinstance(value, int) or isinstance(value, float):
        return {"N": str(value)}
    elif isinstance(value, dict):
        return {"M": {k: convertToDynamodbFormat(v) for k, v in value.items()}}
    elif isinstance(value, list):
        return {"L": [convertToDynamodbFormat(v) for v in value]}
    elif value is None:
        return {"NULL": True}
    elif isinstance(value, Decimal):
        return {"N": str(value)}
    elif hasattr(value, "__dict__"):
        return {"M": {k: convertToDynamodbFormat(v) for k, v in vars(value).items() if k != "stat_chain"}}
    else:
        raise ValueError(f"Unsupported value type: {type(value)}")
def deserializeDynamoDbItem(dynamodbItem):
    return {key: deserializer.deserialize(value) for key, value in dynamodbItem.items()}

def decimal_serializer(obj) :
  if isinstance(obj, Decimal) :
    ratio = obj.as_integer_ratio()
    if ratio[1] == 1 :
      return int(obj)
    return float(obj)
  return obj

# Currently only used in globalUtility, see getSpecificGlobalStatOverTime, can be removed at some point
def fullyJSONifyData(d):
    if isinstance(d, GameAttributeTrie):
        return d.to_dict()
    elif isinstance(d, FrequencyCompiler):
        return d.to_dict()
    elif isinstance(d, Decimal):
        return int(d) if d % 1 == 0 else float(d)
    elif isinstance(d, dict):
        return {key: fullyJSONifyData(value) for key, value in d.items()}
    elif isinstance(d, list):
        return [fullyJSONifyData(item) for item in d]
    elif isinstance(d, set):
        return [fullyJSONifyData(item) for item in sorted(d)]
    elif isinstance(d, tuple):
        return tuple(fullyJSONifyData(item) for item in d)
    else:
        return d

def batchWriteToDynamoDB(items, tableName, dynamodb):
    """Write items to DynamoDB in batches."""
    try:
        # DynamoDB limits batch write to 25 items per request
        MAX_BATCH_SIZE = 25
        for i in range(0, len(items), MAX_BATCH_SIZE):
            batch = items[i:i + MAX_BATCH_SIZE]
            request_items = {
                tableName: [
                    {"PutRequest": {"Item": item}}
                    for item in batch
                ]
            }

            # Write batch to DynamoDB
            response = dynamodb.batch_write_item(RequestItems=request_items)

            # Check for unprocessed items
            while response.get("UnprocessedItems", {}):
                print("Retrying unprocessed items...")
                response = dynamodb.batch_write_item(
                    RequestItems=response["UnprocessedItems"]
                )
    except Exception as e:
        print(f"Error writing batch to DynamoDB: {e.response['Error']['Message']}")

def batch_get_all_items(table_name, keys, dynamodb, projection_expression=None):
    # AI
    request_items = {
        table_name: {
            'Keys': keys
        }
    }
    if projection_expression:
        request_items[table_name]['ProjectionExpression'] = projection_expression

    results = []
    while request_items:
        response = dynamodb.batch_get_item(RequestItems=request_items)
        results.extend(response['Responses'].get(table_name, []))

        unprocessed = response.get('UnprocessedKeys', {})
        request_items = unprocessed if unprocessed else None

    return results