from apiUtility import getApiProxyRecentGames, getApiRecentGames
from DatabaseUtility.itemUtility import batchWriteToDynamoDB, deserializeDynamoDbItem, prepareItemForDB


GAMES_TABLE_NAME = "BrawlStarsGames"
UNCACHED_GAMES_TABLE_NAME = "BrawlStarsUncachedGames"

def getAllUncachedGamesFromDB(playerTag, dynamodb):
    games = []

    response = dynamodb.query(
        TableName=UNCACHED_GAMES_TABLE_NAME,
        KeyConditionExpression="playerTag = :playerTag",
        ExpressionAttributeValues={
            ":playerTag": {"S": playerTag}
        },
    )

    for item in response.get('Items', []):
        games.append(deserializeDynamoDbItem(item))

    # Handle pagination
    while 'LastEvaluatedKey' in response:
        response = dynamodb.query(
            TableName=UNCACHED_GAMES_TABLE_NAME,
            KeyConditionExpression="playerTag = :playerTag",
            ExpressionAttributeValues={
                ":playerTag": {"S": playerTag}
            },
            ExclusiveStartKey=response['LastEvaluatedKey'],
        )
        for item in response.get('Items', []):
            games.append(deserializeDynamoDbItem(item))

    return games

def removeGamesFromUncachedTable(games, dynamodb):
    deleteRequests = []
    for game in games:
        deleteRequests.append({
            "DeleteRequest": {
                "Key": {
                    "playerTag": {"S": game["playerTag"]},
                    "battleTime": {"S": game["battleTime"]}
                }
            }
        })
    
    # Send requets in batches
    for i in range(0, len(deleteRequests), 25):
        batch = {UNCACHED_GAMES_TABLE_NAME: deleteRequests[i:i+25]}
        response = dynamodb.batch_write_item(RequestItems=batch)

        # Retry unprocessed items
        while response.get("UnprocessedItems", {}):
            response = dynamodb.batch_write_item(RequestItems=response["UnprocessedItems"])

def getMostRecentGamesFromDB(playerTag, numGames, useUncachedTable, dynamodb):
    response = dynamodb.query(
        TableName=UNCACHED_GAMES_TABLE_NAME if useUncachedTable else GAMES_TABLE_NAME,
        KeyConditionExpression="playerTag = :playerTag",
        ExpressionAttributeValues={":playerTag": {"S": playerTag}},
        Limit=numGames,
        ScanIndexForward=False
    )

    if response.get('Items'):
        return response['Items']
    else:
        return None

def getMostRecentSavedBattleTime(playerTag, dynamodb):
    mostRecentUncachedGame = getMostRecentGamesFromDB(playerTag, 1, True, dynamodb)
    if mostRecentUncachedGame and len(mostRecentUncachedGame) > 0:
        return mostRecentUncachedGame[0]["battleTime"]["S"]
    
    mostRecentCachedGame = getMostRecentGamesFromDB(playerTag, 1, False, dynamodb)
    if mostRecentCachedGame and len(mostRecentCachedGame) > 0:
        return mostRecentCachedGame[0]["battleTime"]["S"]
    
    return None

def saveGamesFromApiToUncachedDB(playerTag, useProxy, dynamodb):
    recentApiGames = getApiProxyRecentGames(playerTag) if useProxy else getApiRecentGames(playerTag, False)
    if recentApiGames is None:
        return 0

    mostRecentSavedBattleTime = getMostRecentSavedBattleTime(playerTag, dynamodb)

    gamesYetToBeTracked = [
        game for game in recentApiGames
        if mostRecentSavedBattleTime is None or game["battleTime"] > mostRecentSavedBattleTime
    ]

    if len(gamesYetToBeTracked) == 0:
        return 0
    
    for game in gamesYetToBeTracked:
        game["playerTag"] = playerTag

    preparedGames = [prepareItemForDB(game) for game in gamesYetToBeTracked]

    # Sort out ones with same battle times
    seenBattleTimes = set()
    uniquePreparedGames = []
    for item in preparedGames:
        battleTime = item["battleTime"]["S"]

        if battleTime not in seenBattleTimes:
            uniquePreparedGames.append(item)
            seenBattleTimes.add(battleTime)

    batchWriteToDynamoDB(uniquePreparedGames, UNCACHED_GAMES_TABLE_NAME, dynamodb)

    return len(uniquePreparedGames)

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