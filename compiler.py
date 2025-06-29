from decimal import Decimal
import json
import boto3

from datetime import datetime

from CompilerStructuresModule.CompilerStructures.frequencyCompiler import FrequencyCompiler
from CompilerStructuresModule.CompilerStructures.recursiveAttributeStructure import RecursiveAttributeStructure
from DatabaseUtility.gamesUtility import batchWriteGamesToDynamodb, getAllUncachedGames
from DatabaseUtility.itemUtility import deserializeDynamoDbItem, prepareItem
from DatabaseUtility.playerUtility import compileUncachedStats, getAllPlayerTagsSet, getPlayerStatsObject
from brawlStats import BrawlStats


# Initialize DynamoDB client
DYNAMODB_REGION = 'us-west-1'
dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

if __name__ == "__main__":
    print(datetime.now())

    playerTagSet = getAllPlayerTagsSet(dynamodb)

    for player in playerTagSet:
        compileUncachedStats(player, dynamodb)

    print()