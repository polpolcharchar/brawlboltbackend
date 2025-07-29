import boto3
from DatabaseUtility.gamesUtility import GAMES_TABLE_NAME, getMostRecentGamesFromDB
from DatabaseUtility.itemUtility import batchWriteToDynamoDB, prepareItemForDB
from DatabaseUtility.playerUtility import getAllPlayerTagsSetInRecentDays
from apiUtility import getApiRecentGames
from datetime import datetime

def trackRecentUniqueGames(playerTag, dynamodb):
    mostRecentGame = getMostRecentGamesFromDB(playerTag, 1, dynamodb)
    mostRecentBattleTime = mostRecentGame[0]["battleTime"]["S"] if (mostRecentGame and len(mostRecentGame) > 0) else None

    recentGames = getApiRecentGames(playerTag, False)
    if len(recentGames) == 0:
        return 0

    gamesYetToBeTracked = [
        game for game in recentGames
        if not mostRecentBattleTime or game["battleTime"] > mostRecentBattleTime
    ]

    if len(gamesYetToBeTracked) == 0:
        return 0

    for game in gamesYetToBeTracked:
        game["statsCached"] = False
        game["playerTag"] = playerTag
    
    preparedGames = [prepareItemForDB(game) for game in gamesYetToBeTracked]

    # Sometimes there are duplicate battle times, I don't know why
    seenBattleTimes = set()
    uniqueItems = []
    for item in preparedGames:
        battleTime = item["battleTime"]["S"]

        if battleTime not in seenBattleTimes:
            uniqueItems.append(item)
            seenBattleTimes.add(battleTime)

    batchWriteToDynamoDB(uniqueItems, GAMES_TABLE_NAME, dynamodb)

    return len(uniqueItems)

if __name__ == "__main__":
    DYNAMODB_REGION = 'us-west-1'
    dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

    playerTags = getAllPlayerTagsSetInRecentDays(dynamodb, numDays=30)

    numGamesTracked = 0
    for playerTag in playerTags:
        numGamesTracked += trackRecentUniqueGames(playerTag, dynamodb)
    
    print(f"{datetime.now().strftime('%m/%d/%y %I %p')}: {len(playerTags)} players tracked and {numGamesTracked} games saved.")