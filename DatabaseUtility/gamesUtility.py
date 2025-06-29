from DatabaseUtility.itemUtility import deserializeDynamoDbItem


GAMES_TABLE_NAME = "BrawlStarsGames"

# Function to batch write new games to DynamoDB
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
                # print("Retrying unprocessed items...")
                response = dynamodb.batch_write_item(
                    RequestItems=response["UnprocessedItems"]
                )
    except:
        print(f"Error writing batch to DynamoDB")
    
    print("Finished writing " + str(len(items)) + " to DB")

def getAllUncachedGames(playerTag, dynamodb):
    """
    Retrieve all games for a specific player where statsCached is False.

    Parameters:
        playerTag (str): The primary key of the player.

    Returns:
        list: A list of games where statsCached is False.
    """
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

    # Collect games from the response
    for item in response.get('Items', []):
        games.append(deserializeDynamoDbItem(item))

    # If there are more items to retrieve (pagination), keep querying
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