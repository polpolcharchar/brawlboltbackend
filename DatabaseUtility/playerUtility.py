from datetime import datetime
import json
from apiUtility import getApiPlayerInfo
from DatabaseUtility.gamesUtility import batchWriteGamesToDynamodb, getAllUncachedGames, saveRecentGames
from DatabaseUtility.itemUtility import deserializeDynamoDbItem, fullyJSONifyData, prepareItem
from brawlStats import BrawlStats

PLAYER_INFO_TABLE = 'BrawlStarsPlayersInfo'
PLAYER_COMPILED_STATS_TABLE = 'BrawlStarsPlayers2'

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

def getPlayerCompiledStatsJSON(playerTag, dynamodb):
    try:
        # Query all items for the given playerTag
        response = dynamodb.query(
            TableName=PLAYER_COMPILED_STATS_TABLE,
            KeyConditionExpression="playerTag = :playerTag",
            ExpressionAttributeValues={":playerTag": {"S": playerTag}}
        )

        if len(response["Items"]) == 7:
            items = response["Items"]

            # Deserialize each stat type
            stats = {
                item["statType"]["S"]: json.loads(item["stats"]["S"])
                for item in items
            }

            return stats
        else:
            return []
    except Exception as e:
        return []

def getPlayerStatsObject(playerTag, dynamodb):
    try:
        stats = getPlayerCompiledStatsJSON(playerTag, dynamodb)

        if len(stats) == 7:

            playerData = {
                "regular_stat_compilers": {
                    "mode_map_brawler": stats.get("regularModeMapBrawler", {}),
                    "mode_brawler": stats.get("regularModeBrawler", {}),
                    "brawler_mode_map": stats.get("regularBrawlerModeMap", {}),
                },
                "ranked_stat_compilers": {
                    "mode_map_brawler": stats.get("rankedModeMapBrawler", {}),
                    "mode_brawler": stats.get("rankedModeBrawler", {}),
                    "brawler_mode_map": stats.get("rankedBrawlerModeMap", {}),
                },
                "showdown_rank_compilers": stats.get("showdownRankCompilers", {})
            }

            return BrawlStats(False, playerData)
        else:
            print("No stats found")
            return BrawlStats(False)
    except Exception as e:
        print(f"Error loading player stats for {playerTag}: {e}")
        return BrawlStats(False)
 
def compileUncachedStats(playerTag, dynamodb):
    print("Updating stats: " + playerTag)

    #Retrieve Games:
    games = getAllUncachedGames(playerTag, dynamodb)
    print(str(len(games)) + " uncached games")

    if(len(games) == 0):
        return

    #Load and handle stats
    playerStats = getPlayerStatsObject(playerTag, dynamodb)
    playerStats.handleBattles(games, "#" + playerTag)

    # Define the configurations for all stat objects
    rawStatObjects = [
        {
            "playerTag": playerTag,
            "statType": "regularModeMapBrawler",
            "stats": json.dumps(fullyJSONifyData(playerStats.regular_stat_compilers.mode_map_brawler))
        },
        {
            "playerTag": playerTag,
            "statType": "regularModeBrawler",
            "stats": json.dumps(fullyJSONifyData(playerStats.regular_stat_compilers.mode_brawler))
        },
        {
            "playerTag": playerTag,
            "statType": "regularBrawlerModeMap",
            "stats": json.dumps(fullyJSONifyData(playerStats.regular_stat_compilers.brawler_mode_map))
        },
        # Ranked
        {
            "playerTag": playerTag,
            "statType": "rankedModeMapBrawler",
            "stats": json.dumps(fullyJSONifyData(playerStats.ranked_stat_compilers.mode_map_brawler))
        },
        {
            "playerTag": playerTag,
            "statType": "rankedModeBrawler",
            "stats": json.dumps(fullyJSONifyData(playerStats.ranked_stat_compilers.mode_brawler))
        },
        {
            "playerTag": playerTag,
            "statType": "rankedBrawlerModeMap",
            "stats": json.dumps(fullyJSONifyData(playerStats.ranked_stat_compilers.brawler_mode_map))
        },
        #showdown
        {
            "playerTag": playerTag,
            "statType": "showdownRankCompilers",
            #This is weird and needs to be fixed:
            "stats": json.dumps({key: value.to_dict()["frequencies"] for key, value in playerStats.showdown_rank_compilers.items()})
        }
    ]

    preparedStatObjects = [prepareItem(statObject) for statObject in rawStatObjects]

    # Update Player Stats
    for item in preparedStatObjects:
        response = dynamodb.put_item(
            TableName=PLAYER_COMPILED_STATS_TABLE,
            Item=item,
            # ReturnConsumedCapacity='TOTAL'  # Adjust this value as needed
        )

    #set statsCached for every game to false
    for game in games:
        game['statsCached'] = True

    #Update all games
    preparedGames = [prepareItem(game) for game in games]
    batchWriteGamesToDynamodb(preparedGames, dynamodb)

    print(playerTag + " updated.")

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
    apiPlayerInfo = getApiPlayerInfo(playerTag)

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

#Unused, remove?
def isLastCompiledOlderThan(playerTag, days, dynamodb):
    """
    Check if the last compiled date for a player is older than a specified number of days.

    Parameters:
        playerTag (str): The primary key of the player.
        days (int): The number of days to check against.

    Returns:
        bool: True if the last compiled date is older than the specified number of days, False otherwise.
    """
    try:
        # Query the table for the playerTag
        response = dynamodb.query(
            TableName=PLAYER_INFO_TABLE,
            KeyConditionExpression="playerTag = :playerTag",
            ExpressionAttributeValues={":playerTag": {"S": playerTag}}
        )

        # Check if the item exists and if the last compiled date is older than the specified number of days
        if len(response["Items"]) > 0:
            lastCompiledDate = datetime.fromisoformat(response["Items"][0]["statsLastCompiled"]["S"])
            return (datetime.now() - lastCompiledDate).days > days
        else:
            # No stats found for the player, return True
            # print("Player not found, returning True")
            return True
    except Exception as e:
        # print(f"Error checking last compiled date for {playerTag}: {e}")
        return True


