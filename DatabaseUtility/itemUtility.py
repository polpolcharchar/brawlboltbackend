from decimal import Decimal
from boto3.dynamodb.types import TypeDeserializer

from DatabaseUtility.capacityHandler import handleCapacity

deserializer = TypeDeserializer()

def prepareItemForDB(game):
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
    elif isinstance(value, int) or isinstance(value, float) or isinstance(value, Decimal):
        return {"N": str(value)}
    elif isinstance(value, dict):
        return {"M": {k: convertToDynamodbFormat(v) for k, v in value.items()}}
    elif isinstance(value, list):
        return {"L": [convertToDynamodbFormat(v) for v in value]}
    elif value is None:
        return {"NULL": True}
    elif isinstance(value, (set, frozenset)):
        if not value:
            return None #Skip empty
        
        elem_types = {type(v) for v in value}

        if len(elem_types) > 1:
            raise ValueError(f"Mixed-type sets are not supported by DynamoDB: {elem_types}")

        elem_type = elem_types.pop()
        if issubclass(elem_type, str):
            return {"SS": list(value)}
        elif issubclass(elem_type, (int, float, Decimal)):
            return {"NS": [str(v) for v in value]}
        elif issubclass(elem_type, bytes):
            return {"BS": list(value)}
        else:
            raise ValueError(f"Unsupported set element type for DynamoDB: {elem_type}")

    elif hasattr(value, "__dict__"):
        return {"M": {
            k: convertToDynamodbFormat(v)
            for k, v in vars(value).items()
            if k != "stat_chain"
        }}
    else:
        raise ValueError(f"Unsupported value type: {type(value)}")
def deserializeDynamoDbItem(dynamodbItem):
    return {key: deserializer.deserialize(value) for key, value in dynamodbItem.items()}

def decimalAndSetSerializer(obj):
    if isinstance(obj, Decimal):
        ratio = obj.as_integer_ratio()
        if ratio[1] == 1:
            return int(obj)
        return float(obj)
    elif isinstance(obj, set):
        return list(obj)
    return obj

def batchWriteToDynamoDB(items, tableName, dynamodb):
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
            response = dynamodb.batch_write_item(RequestItems=request_items, ReturnConsumedCapacity='TOTAL')
            # handleCapacity(response, "batchWriteToDynamoDB")
            print(response)

            # Check for unprocessed items
            while response.get("UnprocessedItems", {}):
                print("Retrying unprocessed items...")
                response = dynamodb.batch_write_item(
                    RequestItems=response["UnprocessedItems"],
                    ReturnConsumedCapacity='TOTAL'
                )
                # handleCapacity(response, "batchWriteToDynamoDB")
                print(response)
    except Exception as e:
        print(f"Error writing batch to DynamoDB: {e.response['Error']['Message']}")

def batchGetAllItems(table_name, keys, dynamodb, projection_expression=None):
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