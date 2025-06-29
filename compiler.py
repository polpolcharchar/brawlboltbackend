from decimal import Decimal
import json
import boto3
from boto3.dynamodb.types import TypeDeserializer

import pickle

from datetime import datetime

from CompilerStructuresModule.CompilerStructures.frequencyCompiler import FrequencyCompiler
from CompilerStructuresModule.CompilerStructures.recursiveAttributeStructure import RecursiveAttributeStructure
from brawlStats import BrawlStats


DYNAMODB_REGION = 'us-west-1'
GAMES_TABLE_NAME = "BrawlStarsGames"
playerCompiledStatsTable = 'BrawlStarsPlayers2'
playerInfoTable = 'BrawlStarsPlayersInfo'

# Initialize DynamoDB client
dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)
deserializer = TypeDeserializer()

# Function to prepare and unprepare DynamoDB items
def prepareItem(game):
    item = {}
    for key, value in game.items():
        item[key] = convertToDynamodbFormat(value)
    return item

def convertToDynamodbFormat(value):
    if isinstance(value, str):
        return {"S": value}
    elif isinstance(value, bytes):
        return {"B": value}
    elif isinstance(value, bool):
        return {"BOOL": value}
    elif isinstance(value, int) or isinstance(value, float):
        return {"N": str(value)}
    elif isinstance(value, dict):
        return {"M": {k: convertToDynamodbFormat(v) for k, v in value.items()}}
    elif isinstance(value, list):
        return {"L": [convertToDynamodbFormat(v) for v in value]}
    elif value is None:
        return {"NULL": True}
    elif isinstance(value, Decimal):
        return {"N": str(value)}
    elif hasattr(value, "__dict__"):  # Check if it's a class instance
        return {"M": {k: convertToDynamodbFormat(v) for k, v in vars(value).items() if k != "stat_chain"}}
    else:
        raise ValueError(f"Unsupported value type: {type(value)}")
def deserializeDynamoDbItem(dynamodbItem):
    return {key: deserializer.deserialize(value) for key, value in dynamodbItem.items()}

# Function to batch write new games to DynamoDB
def batchWriteGamesToDynamodb(items):
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

            # print(f"Batch {i // MAX_BATCH_SIZE + 1} finished of {len(items) // MAX_BATCH_SIZE + (1 if len(items) % MAX_BATCH_SIZE > 0 else 0)}")
    except:
        print(f"Error writing batch to DynamoDB")
    
    print("Finished writing " + str(len(items)) + " to DB")

def getAllPlayersInfo(projectionExpression):

    # Initialize an empty list to store playerTags
    playersInfo = []

    # Use a scan operation to retrieve all items from the table
    response = dynamodb.scan(
        TableName=playerInfoTable,
        ProjectionExpression=projectionExpression
    )

    # Collect playerTags from the response
    for item in response.get('Items', []):
        playersInfo.append(deserializeDynamoDbItem(item))

    # If there are more items to retrieve (pagination), keep scanning
    while 'LastEvaluatedKey' in response:
        response = dynamodb.scan(
            TableName=playerInfoTable,
            ExclusiveStartKey=response['LastEvaluatedKey'],
            ProjectionExpression=projectionExpression
        )
        for item in response.get('Items', []):
            playersInfo.append(deserializeDynamoDbItem(item))

    return playersInfo

def getAllUncachedGames(playerTag):
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

def getPlayerStatsObject(playerTag):
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

def compileUncachedStats(playerTag):
    print("Updating stats: " + playerTag)

    #Retrieve Games:
    games = getAllUncachedGames(playerTag)
    print(str(len(games)) + " uncached games")

    if(len(games) == 0):
        return

    #Load and handle stats
    playerStats = getPlayerStatsObject(playerTag)
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
    batchWriteGamesToDynamodb(preparedGames)

    print(playerTag + " updated.")

# Main script
if __name__ == "__main__":
    print(datetime.now())

    playersInfo = getAllPlayersInfo("playerTag")

    playerTagSet = set()
    for info in playersInfo:
        playerTagSet.add(info['playerTag'])

    for player in playerTagSet:
        compileUncachedStats(player)

    print()