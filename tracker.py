import boto3
from DatabaseUtility.gamesUtility import saveGamesFromApiToUncachedDB
from DatabaseUtility.playerUtility import getAllPlayerTagsSetInRecentDays
from datetime import datetime

if __name__ == "__main__":
    DYNAMODB_REGION = 'us-west-1'
    dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

    playerTags = getAllPlayerTagsSetInRecentDays(dynamodb, numDays=30)

    numGamesTracked = 0
    for playerTag in playerTags:
        numGamesTracked += saveGamesFromApiToUncachedDB(playerTag, False, dynamodb)
    
    print(f"{datetime.now().strftime('%m/%d/%y %I %p')}: {len(playerTags)} players tracked and {numGamesTracked} games saved.")