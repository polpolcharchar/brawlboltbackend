from decimal import Decimal
from boto3.dynamodb.types import TypeDeserializer

deserializer = TypeDeserializer()

# Function to prepare and unprepare DynamoDB items
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
    elif hasattr(value, "__dict__"):  # Check if it's a class instance
        return {"M": {k: convertToDynamodbFormat(v) for k, v in vars(value).items() if k != "stat_chain"}}
    else:
        raise ValueError(f"Unsupported value type: {type(value)}")
def deserializeDynamoDbItem(dynamodbItem):
    return {key: deserializer.deserialize(value) for key, value in dynamodbItem.items()}
