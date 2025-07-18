from datetime import datetime, timedelta
from DatabaseUtility.trieUtility import getMatchDataObjectsFromGame, updateDatabaseTrie
from apiUtility import getApiProxyPlayerInfo
from DatabaseUtility.gamesUtility import batchWriteGamesToDynamodb, getAllUncachedGames, saveRecentGames
from DatabaseUtility.itemUtility import deserializeDynamoDbItem, prepareItem

PLAYER_INFO_TABLE = 'BrawlStarsPlayersInfo'

def getAllPlayerTagsSet(dynamodb):

    playersInfo = []

    response = dynamodb.scan(
        TableName=PLAYER_INFO_TABLE,
    )

    # Collect playerTags from the response
    for item in response.get('Items', []):
        playersInfo.append(deserializeDynamoDbItem(item))

    # If there are more items to retrieve (pagination), keep scanning
    while 'LastEvaluatedKey' in response:
        response = dynamodb.scan(
            TableName=PLAYER_INFO_TABLE,
            ExclusiveStartKey=response['LastEvaluatedKey'],
        )
        for item in response.get('Items', []):
            playersInfo.append(deserializeDynamoDbItem(item))
    
    playerTagSet = set()
    for info in playersInfo:
        playerTagSet.add(info['playerTag'])

    return playerTagSet

def getAllPlayerTagsSetInRecentDays(dynamodb, numDays=30):
    cutoffDate = datetime.utcnow() - timedelta(days=numDays)

    tags = set()
    response = dynamodb.scan(
        TableName=PLAYER_INFO_TABLE,
        ProjectionExpression='playerTag, statsLastAccessed'
    )

    while True:
        for item in response.get('Items', []):
            player_tag = item['playerTag']['S']
            stats_last_accessed = item.get('statsLastAccessed', {}).get('S')
            if stats_last_accessed:
                last_accessed_date = datetime.fromisoformat(stats_last_accessed)
                if last_accessed_date >= cutoffDate:
                    tags.add(player_tag)

        # Check for more items to scan
        if 'LastEvaluatedKey' not in response:
            break
        response = dynamodb.scan(
            TableName=PLAYER_INFO_TABLE,
            ExclusiveStartKey=response['LastEvaluatedKey'],
            ProjectionExpression='playerTag, statsLastAccessed'
        )

    return tags

def compileUncachedStats(playerTag, dynamodb):
    print(playerTag + ": ", end="")

    #Retrieve Games:
    games = getAllUncachedGames(playerTag, dynamodb)
    print(str(len(games)) + " uncached games, ", end="")

    if(len(games) == 0):
        print()
        return

    matchDataObjects = []
    for game in games:
        matchDataObjects.extend(getMatchDataObjectsFromGame(game, "#" + playerTag, False))

    updateDatabaseTrie(playerTag, matchDataObjects, "overall", dynamodb, False, False)

    #set statsCached for every game to false
    for game in games:
        game['statsCached'] = True

    #Update all games
    preparedGames = [prepareItem(game) for game in games]
    batchWriteGamesToDynamodb(preparedGames, dynamodb)
    print(f"cached {len(preparedGames)} games, ", end="")

    updateStatsLastCompiled("9CUCYLQP", dynamodb)

    print("finished")

def updateStatsLastCompiled(playerTag, dynamodb):
    dynamodb.update_item(
        TableName=PLAYER_INFO_TABLE,
        Key={"playerTag": {"S": playerTag}},
        UpdateExpression="SET statsLastCompiled = :statsLastCompiled",
        ExpressionAttributeValues={":statsLastCompiled": {"S": datetime.now().isoformat()}}
    )

def updateStatsLastAccessed(playerTag, dynamodb):
    dynamodb.update_item(
        TableName=PLAYER_INFO_TABLE,
        Key={"playerTag": {"S": playerTag}},
        UpdateExpression="SET statsLastAccessed = :statsLastAccessed",
        ExpressionAttributeValues={":statsLastAccessed": {"S": datetime.now().isoformat()}}
    )

def beginTrackingPlayer(playerTag, dynamodb):
    
    #Check that this is a valid playerTag:
    apiPlayerInfo = getApiProxyPlayerInfo(playerTag)

    if apiPlayerInfo is None:
        return False

    dynamodb.put_item(
        TableName=PLAYER_INFO_TABLE,
        Item={
            'playerTag': {'S': playerTag},
            'currentlyTrackingGames': {'BOOL': True},
            'regularlyCompileStats': {'BOOL': False},
            'username': {'S': apiPlayerInfo['name']},
            'statsLastCompiled': {'S': datetime.min.isoformat()},
            'statsLastAccessed': {'S': datetime.now().isoformat()}
        },
    )

    saveRecentGames(playerTag, dynamodb)

    return True

def getPlayerInfo(playerTag, dynamodb):
    return dynamodb.query(
        TableName=PLAYER_INFO_TABLE,
        KeyConditionExpression='playerTag = :playerTag',
        ExpressionAttributeValues={
            ':playerTag': {'S': playerTag},
        },
    )
