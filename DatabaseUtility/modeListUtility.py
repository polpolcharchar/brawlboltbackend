from DatabaseUtility.trieUtility import fetchRecentTrieData


MODE_LIST_TABLE = "BrawlStarsModes"

# Intended to be run locally
def cacheModeList(dynamodb):
    rawModeList = [trieNode["pathID"].split("$")[-1] for trieNode in fetchRecentTrieData("global", 1, True, "regular", None, None, None, "mode", dynamodb)[0]["trieData"]]

    currentModeList = getCachedModeList(dynamodb)

    #Merge with current modes to avoid losing any modes that may not be in the recent trie data
    if currentModeList is not None:
        rawModeList = list(set(rawModeList) | set(currentModeList))

    dynamodb.update_item(
        TableName=MODE_LIST_TABLE,
        Key={
            "id": {"S": "main"},
        },
        UpdateExpression="SET modeNames = :modeNames",
        ExpressionAttributeValues={
            ":modeNames": {"SS": rawModeList},
        }
    )

def getCachedModeList(dynamodb):
    response = dynamodb.get_item(
        TableName=MODE_LIST_TABLE,
        Key={
            "id": {"S": "main"},
        },
    )

    if 'Item' not in response:
        return None

    item = response['Item']
    if 'modeNames' not in item:
        return None

    modeNames = item['modeNames'].get('SS', [])
    return modeNames