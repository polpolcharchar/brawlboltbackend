from datetime import datetime, timedelta
from DatabaseUtility.modeToMapOverrideUtility import getMode
from DatabaseUtility.trieUtility import fetchTrieData, getMatchDataObjectsFromGame, updateDatabaseTrie
from apiUtility import getApiProxyPlayerInfo
from DatabaseUtility.gamesUtility import GAMES_TABLE_NAME, getAllUncachedGamesFromDB, getBrawlers, getMostRecentGamesFromDB, saveRecentGamesFromApiToDB
from DatabaseUtility.itemUtility import batchWriteToDynamoDB, deserializeDynamoDbItem, prepareItemForDB

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
    games = getAllUncachedGamesFromDB(playerTag, dynamodb)
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
    preparedGames = [prepareItemForDB(game) for game in games]
    batchWriteToDynamoDB(preparedGames, GAMES_TABLE_NAME, dynamodb)
    # print(f"cached {len(preparedGames)} games, ", end="")

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

    saveRecentGamesFromApiToDB(playerTag, dynamodb)

    return True

def getPlayerInfo(playerTag, dynamodb):
    return dynamodb.query(
        TableName=PLAYER_INFO_TABLE,
        KeyConditionExpression='playerTag = :playerTag',
        ExpressionAttributeValues={
            ':playerTag': {'S': playerTag},
        },
    )

def getPlayerOverview(playerTag, dynamodb):
    
    # Get most recent 10 games
    rawRecentGames = getMostRecentGamesFromDB(playerTag, 10, dynamodb)

    lastSeenString = rawRecentGames[0]["battleTime"]["S"]
    lastSeen = datetime.strptime(lastSeenString, "%Y%m%dT%H%M%S.%fZ")
    daysSinceLastSeen = max(0, int((datetime.now() - lastSeen).days))

    parsedRecentGames = []
    for game in rawRecentGames:
        deserializedGame = deserializeDynamoDbItem(game)

        parsedGame = {
            "mode": getMode(deserializedGame),
            "type": deserializedGame["battle"]["type"],
            "brawlers": getBrawlers(deserializedGame, playerTag)
        }
        if "result" in deserializedGame["battle"]:
            parsedGame["result"] = deserializedGame["battle"]["result"]
        if "rank" in deserializedGame["battle"]:
            parsedGame["rank"] = deserializedGame["battle"]["rank"]
        
        parsedRecentGames.append(parsedGame)

    # Get favorite brawlers + winrates
    brawlerTrieResult = fetchTrieData(playerTag, "overall", "regular", None, None, None, "brawler", False, dynamodb)["trieData"]
    
    brawlerRatesList = []
    for brawlerNode in brawlerTrieResult:
        brawler = brawlerNode["pathID"].split("$")[-1]

        rateObject = {
            "brawler": brawler,
            "winrate": float(brawlerNode["resultCompiler"]["player_result_data"]["wins"] / brawlerNode["resultCompiler"]["player_result_data"]["potential_total"]).__round__(3),       
            "numGames": float(brawlerNode["resultCompiler"]["player_result_data"]["potential_total"])
        }
        if brawlerNode["resultCompiler"]["player_star_data"]["potential_total"] > 0:
            rateObject["starRate"] = float((
                brawlerNode["resultCompiler"]["player_star_data"]["wins"] + 
                brawlerNode["resultCompiler"]["player_star_data"]["draws"] + 
                brawlerNode["resultCompiler"]["player_star_data"]["losses"]
            ) / brawlerNode["resultCompiler"]["player_star_data"]["potential_total"]).__round__(3)

        brawlerRatesList.append(rateObject)
    
    favoriteBrawlers = sorted(brawlerRatesList, key=lambda x: x["numGames"], reverse=True)[:10]

    # Get favorite modes + winrates
    modeTrieResult = fetchTrieData(playerTag, "overall", "regular", None, None, None, "mode", False, dynamodb)["trieData"]
    
    modeRatesList = []
    for modeNode in modeTrieResult:
        mode = modeNode["pathID"].split("$")[-1]

        rateObject = {
            "mode": mode,
            "winrate": float(modeNode["resultCompiler"]["player_result_data"]["wins"] / modeNode["resultCompiler"]["player_result_data"]["potential_total"]).__round__(3),       
            "numGames": float(modeNode["resultCompiler"]["player_result_data"]["potential_total"])
        }
        if modeNode["resultCompiler"]["player_star_data"]["potential_total"] > 0:
            rateObject["starRate"] = float((
                modeNode["resultCompiler"]["player_star_data"]["wins"] + 
                modeNode["resultCompiler"]["player_star_data"]["draws"] + 
                modeNode["resultCompiler"]["player_star_data"]["losses"]
            ) / modeNode["resultCompiler"]["player_star_data"]["potential_total"]).__round__(3)

        modeRatesList.append(rateObject)
    
    favoriteModes = sorted(modeRatesList, key=lambda x: x["numGames"], reverse=True)[:10]

    return {
        "parsedRecentGames": parsedRecentGames,
        "favoriteBrawlers": favoriteBrawlers,
        "favoriteModes": favoriteModes,
        "daysSinceLastSeen": daysSinceLastSeen
    }