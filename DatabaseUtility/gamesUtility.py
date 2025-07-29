from apiUtility import getApiProxyRecentGames
from DatabaseUtility.itemUtility import batchWriteToDynamoDB, deserializeDynamoDbItem, prepareItemForDB


GAMES_TABLE_NAME = "BrawlStarsGames"

def getAllUncachedGamesFromDB(playerTag, dynamodb):
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

def getMostRecentGamesFromDB(playerTag, numGames, dynamodb):
    response = dynamodb.query(
        TableName=GAMES_TABLE_NAME,
        KeyConditionExpression="playerTag = :playerTag",
        ExpressionAttributeValues={":playerTag": {"S": playerTag}},
        Limit=numGames,
        ScanIndexForward=False
    )

    if response.get('Items'):
        return response['Items']
    else:
        return None

def saveRecentGamesFromApiToDB(playerTag, dynamodb):
    recentApiGames = getApiProxyRecentGames(playerTag)

    if recentApiGames is None:
        recentApiGames = []
    
    mostRecentCachedGame = getMostRecentGamesFromDB(playerTag, 1, dynamodb)
    mostRecentGameTime = mostRecentCachedGame[0]["battleTime"]["S"] if (mostRecentCachedGame and len(mostRecentCachedGame) > 0) else None

    newGames = [
        game for game in recentApiGames
        if not mostRecentGameTime or game["battleTime"] > mostRecentGameTime
    ]

    for game in newGames:
        game["statsCached"] = False
        game["playerTag"] = playerTag

    preparedGames = [prepareItemForDB(game) for game in newGames]

    batchWriteToDynamoDB(preparedGames, GAMES_TABLE_NAME, dynamodb)

def queryGames(player_tag: str, battle_time: str, num_before: int, num_after: int, dynamodb):
    resultGames = []

    # Get games before the target time (descending order)
    if num_before > 0:
        before_response = dynamodb.query(
            TableName=GAMES_TABLE_NAME,
            KeyConditionExpression="playerTag = :pt AND battleTime < :target",
            ExpressionAttributeValues={
                ":pt": {"S": player_tag},
                ":target": {"S": battle_time}
            },
            ScanIndexForward=False,  # descending
            Limit=min(num_before, 10)
        )

        before_games = [deserializeDynamoDbItem(game) for game in before_response.get("Items", [])]
        resultGames.extend(reversed(before_games))  # Add in ascending order

    # Get games after the target time (ascending order)
    if num_after > 0:
        after_response = dynamodb.query(
            TableName=GAMES_TABLE_NAME,
            KeyConditionExpression="playerTag = :pt AND battleTime > :target",
            ExpressionAttributeValues={
                ":pt": {"S": player_tag},
                ":target": {"S": battle_time}
            },
            ScanIndexForward=True,  # ascending
            Limit=min(num_after, 10)
        )

        for game in after_response.get("Items", []):
            resultGames.append(deserializeDynamoDbItem(game))

    return resultGames

def getBrawlers(game, playerTag):
    tagWithHash = "#" + playerTag

    targetPlayer = None
    if "players" in game["battle"]:
        for player in game["battle"]["players"]:
            if player["tag"] == tagWithHash:
                targetPlayer = player
                break
    elif "teams" in game["battle"]:
        for team in game["battle"]["teams"]:
            for player in team:
                if player["tag"] == tagWithHash:
                    targetPlayer = player
                    break
    
    if targetPlayer is None:
        return None
    
    if "brawler" in targetPlayer:
        return [targetPlayer["brawler"]["name"]]
    else:
        return [brawler["name"] for brawler in targetPlayer["brawlers"]]