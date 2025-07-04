from decimal import Decimal
from boto3.dynamodb.types import TypeDeserializer

from CompilerStructuresModule.CompilerStructures.frequencyCompiler import FrequencyCompiler
from CompilerStructuresModule.CompilerStructures.recursiveAttributeStructure import RecursiveAttributeStructure

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

#Used for getting rid of decimals and weird sustaining RecursiveAttributeStructure
#Should be improved to removal at some point
def fullyJSONifyData(d):
    if isinstance(d, RecursiveAttributeStructure):
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