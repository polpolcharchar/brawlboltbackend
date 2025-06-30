import boto3
from DatabaseUtility.gamesUtility import GAMES_TABLE_NAME, getMostRecentGame
from DatabaseUtility.itemUtility import batchWriteToDynamoDB, prepareItem
from DatabaseUtility.playerUtility import getAllPlayerTagsSetInRecentDays
from apiUtility import getApiRecentGames

def trackRecentUniqueGames(playerTag, dynamodb):
    mostRecentGame = getMostRecentGame(playerTag, dynamodb)
    mostRecentBattleTime = mostRecentGame["battleTime"]["S"] if mostRecentGame else None

    recentGames = getApiRecentGames(playerTag)
    if len(recentGames) == 0:
        print(f"No recent games found for {playerTag}.")
        return

    gamesYetToBeTracked = [
        game for game in recentGames
        if not mostRecentBattleTime or game["battleTime"] > mostRecentBattleTime
    ]

    if len(gamesYetToBeTracked) == 0:
        print(f"No uncached games for {playerTag}.")
        return

    for game in gamesYetToBeTracked:
        game["statsCached"] = False
        game["playerTag"] = playerTag
    
    preparedGames = [prepareItem(game) for game in gamesYetToBeTracked]

    # Sometimes there are duplicate battle times, I don't know why
    seenBattleTimes = set()
    uniqueItems = []
    for item in preparedGames:
        battleTime = item["battleTime"]["S"]

        if battleTime not in seenBattleTimes:
            uniqueItems.append(item)
            seenBattleTimes.add(battleTime)

    batchWriteToDynamoDB(uniqueItems, GAMES_TABLE_NAME, dynamodb)

    print(f"{playerTag}: {len(uniqueItems)}")

if __name__ == "__main__":
    DYNAMODB_REGION = 'us-west-1'
    dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

    playerTags = getAllPlayerTagsSetInRecentDays(dynamodb, numDays=60)

    for playerTag in playerTags:
        trackRecentUniqueGames(playerTag, dynamodb)