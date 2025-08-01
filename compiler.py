import boto3
from datetime import datetime

from DatabaseUtility.playerUtility import compileUncachedStats, getAllPlayerTagsSet


DYNAMODB_REGION = 'us-west-1'
dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

if __name__ == "__main__":
    print("Beginning Compilation at " + str(datetime.now()))

    playerTagSet = getAllPlayerTagsSet(dynamodb)

    for player in playerTagSet:
        compileUncachedStats(player, dynamodb)

    print()