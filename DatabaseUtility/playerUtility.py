

import json
from CompilerStructuresModule.CompilerStructures.frequencyCompiler import FrequencyCompiler
from CompilerStructuresModule.CompilerStructures.recursiveAttributeStructure import RecursiveAttributeStructure
from DatabaseUtility.gamesUtility import batchWriteGamesToDynamodb, getAllUncachedGames
from DatabaseUtility.itemUtility import deserializeDynamoDbItem, prepareItem
from brawlStats import BrawlStats


playerInfoTable = 'BrawlStarsPlayersInfo'
playerCompiledStatsTable = 'BrawlStarsPlayers2'


def getAllPlayerTagsSet(dynamodb):

    # Initialize an empty list to store playerTags
    playersInfo = []

    # Use a scan operation to retrieve all items from the table
    response = dynamodb.scan(
        TableName=playerInfoTable,
    )

    # Collect playerTags from the response
    for item in response.get('Items', []):
        playersInfo.append(deserializeDynamoDbItem(item))

    # If there are more items to retrieve (pagination), keep scanning
    while 'LastEvaluatedKey' in response:
        response = dynamodb.scan(
            TableName=playerInfoTable,
            ExclusiveStartKey=response['LastEvaluatedKey'],
        )
        for item in response.get('Items', []):
            playersInfo.append(deserializeDynamoDbItem(item))
    
    playerTagSet = set()
    for info in playersInfo:
        playerTagSet.add(info['playerTag'])

    return playerTagSet

def getPlayerStatsObject(playerTag, dynamodb):
    try:
        # Query all items for the given playerTag
        response = dynamodb.query(
            TableName=playerCompiledStatsTable,
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

            # Create and return a PlayerStats object using the deserialized data
            return BrawlStats(False, playerData)
        else:
            # No stats found for the player, return a new blank object
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


    def fullyJSONifyData(d):
        if isinstance(d, RecursiveAttributeStructure):
            return d.to_dict()
        elif isinstance(d, FrequencyCompiler):
            return d.to_dict()
        elif isinstance(d, dict):
            return {key: fullyJSONifyData(value) for key, value in d.items()}
        elif isinstance(d, list):
            return [fullyJSONifyData(item) for item in d]
        elif isinstance(d, set):
            return [fullyJSONifyData(item) for item in sorted(d)]  # Convert to a sorted list for JSON compatibility
        elif isinstance(d, tuple):
            return tuple(fullyJSONifyData(item) for item in d)
        else:
            # Return primitive types (str, int, float, bool, None) as is
            return d

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
            TableName=playerCompiledStatsTable,
            Item=item,
            # ReturnConsumedCapacity='TOTAL'  # Adjust this value as needed
        )
        # print(response)

    #set statsCached for every game to false
    for game in games:
        game['statsCached'] = True

    #Update all games
    preparedGames = [prepareItem(game) for game in games]
    batchWriteGamesToDynamodb(preparedGames, dynamodb)

    print(playerTag + " updated.")

