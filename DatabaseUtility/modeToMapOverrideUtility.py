import json
import boto3

_mapToModeOverrides = None
def getMode(game):
    def fetchAndAssignOverrides():
        global _mapToModeOverrides

        DYNAMODB_REGION = 'us-west-1'
        MAP_TO_MODE_OVERRIDES_TABLE = "BrawlStarsMapToModeOverrides"
        dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

        try:
            response = dynamodb.get_item(
                TableName=MAP_TO_MODE_OVERRIDES_TABLE,
                Key={'overrideType': {'S': 'standard'}}
            )

            if 'Item' in response and 'overrides' in response['Item']:
                overrides_raw = response['Item']['overrides']['S']
                overrides = json.loads(overrides_raw)
                _mapToModeOverrides = overrides
            else:
                print("Overrides not found.")
                _mapToModeOverrides = {}
        except Exception as e:
            print(f"An error occurred: {e}")
            _mapToModeOverrides = {}

    if _mapToModeOverrides is None:
        print("Fetching")
        fetchAndAssignOverrides()

    if 'map' in game['event'] and game['event']['map'] in _mapToModeOverrides:
        return _mapToModeOverrides[game['event']['map']]
    elif 'mode' in game['event'] and game['event']['mode'] != "unknown":
        return game['event']['mode']
    elif 'mode' in game['battle']:
        return game['battle']['mode']
    else:
        return "unknown"