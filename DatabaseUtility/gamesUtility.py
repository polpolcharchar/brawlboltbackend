from apiUtility import getApiProxyRecentGames
from DatabaseUtility.itemUtility import deserializeDynamoDbItem, prepareItem


GAMES_TABLE_NAME = "BrawlStarsGames"

def batchWriteGamesToDynamodb(items, dynamodb):
    """Write items to DynamoDB in batches."""
    try:
        # DynamoDB limits batch write to 25 items per request
        MAX_BATCH_SIZE = 25
        for i in range(0, len(items), MAX_BATCH_SIZE):
            batch = items[i:i + MAX_BATCH_SIZE]
            request_items = {
                GAMES_TABLE_NAME: [
                    {"PutRequest": {"Item": item}}
                    for item in batch
                ]
            }

            # Write batch to DynamoDB
            response = dynamodb.batch_write_item(RequestItems=request_items)

            # Check for unprocessed items
            while response.get("UnprocessedItems", {}):
                response = dynamodb.batch_write_item(
                    RequestItems=response["UnprocessedItems"]
                )
    except:
        print(f"Error writing batch to DynamoDB")
    
    print("Finished writing " + str(len(items)) + " to DB")

def getAllUncachedGames(playerTag, dynamodb):
    games = []

    # Query the table for the playerTag with a filter for statsCached = False
    response = dynamodb.query(
        TableName=GAMES_TABLE_NAME,
        KeyConditionExpression="playerTag = :playerTag",
        FilterExpression="statsCached = :statsCached",
        ExpressionAttributeValues={
            ":playerTag": {"S": playerTag},
            ":statsCached": {"BOOL": False}
        }
    )

    for item in response.get('Items', []):
        games.append(deserializeDynamoDbItem(item))

    while 'LastEvaluatedKey' in response:
        response = dynamodb.query(
            TableName=GAMES_TABLE_NAME,
            KeyConditionExpression="playerTag = :playerTag",
            FilterExpression="statsCached = :statsCached",
            ExpressionAttributeValues={
                ":playerTag": {"S": playerTag},
                ":statsCached": {"BOOL": False}
            },
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        for item in response.get('Items', []):
            games.append(deserializeDynamoDbItem(item))

    return games

def getMostRecentGame(playerTag, dynamodb):
    response = dynamodb.query(
        TableName=GAMES_TABLE_NAME,
        KeyConditionExpression="playerTag = :playerTag",
        ExpressionAttributeValues={":playerTag": {"S": playerTag}},
        Limit=1,
        ScanIndexForward=False
    )

    if response.get('Items'):
        return response['Items'][0]
    else:
        return None

def saveRecentGames(playerTag, dynamodb):
    recentApiGames = getApiProxyRecentGames(playerTag)

    if recentApiGames is None:
        recentApiGames = []
    
    mostRecentCachedGame = getMostRecentGame(playerTag, dynamodb)
    mostRecentGameTime = mostRecentCachedGame["battleTime"]["S"] if mostRecentCachedGame else None

    newGames = [
        game for game in recentApiGames
        if not mostRecentGameTime or game["battleTime"] > mostRecentGameTime
    ]

    for game in newGames:
        game["statsCached"] = False
    
    for game in newGames:
        game["playerTag"] = playerTag
    preparedGames = [prepareItem(game) for game in newGames]

    batchWriteGamesToDynamodb(preparedGames, dynamodb)