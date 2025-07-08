import json

from CompilerStructuresModule.CompilerStructures.globalResultCompiler import GlobalResultCompiler
from CompilerStructuresModule.CompilerStructures.playerResultCompiler import PlayerResultCompiler
from DatabaseUtility.itemUtility import batchWriteToDynamoDB, prepareItem
from DatabaseUtility.playerUtility import getPlayerStatsObject

BRAWL_TRIE_TABLE = "BrawlTrieStorage"

def saveOldPlayerStatsObjectToTrieDatabase(playerTag, dynamodb):
    brawlStats = getPlayerStatsObject(playerTag, dynamodb)
    tries = {
                "$modeBrawler": brawlStats.typeModeBrawler, 
                "$modeMapBrawler": brawlStats.typeModeMapBrawler, 
                "$brawlerModeMap": brawlStats.typeBrawlerModeMap
            }
    for key, value in tries.items():
        jsonified = value.to_dict()
        saveTrie(jsonified, playerTag + key, dynamodb)

def saveTrie(gameAttributeTrieJSON, basePath, dynamodb):
    allItems = getAllTrieNodeItems(gameAttributeTrieJSON, basePath, dynamodb)

    preparedItems = [prepareItem(item) for item in allItems]

    print(f"Saving {len(preparedItems)} items... ", end="")
    batchWriteToDynamoDB(preparedItems, BRAWL_TRIE_TABLE, dynamodb)
    print("Done.")

def getAllTrieNodeItems(gameAttributeTrieJSON, currentPath, dynamodb):

    items = []
    childrenPathIDs = []

    for key, value in gameAttributeTrieJSON.get("stat_map", {}).items():
        # Key is the next attribute, like Colt, Ranked, or brawlBall
        # Value is the gameAttributeTrie

        childPath = currentPath + "$" + key

        childrenPathIDs.append(childPath)

        childrenItems = getAllTrieNodeItems(value, childPath, dynamodb)
        items.extend(childrenItems)
    
    items.append(getTrieNodeItem(gameAttributeTrieJSON["overall"], currentPath, childrenPathIDs))

    return items
    
def getTrieNodeItem(resultCompilerJSON, pathID, childrenPathIDs):
    item = {
        "pathID": pathID,
        "datetime": "temp",
        "childrenPathIDs": childrenPathIDs,
        "resultCompiler": resultCompilerJSON
    }

    return item


def updateDatabaseTrie(basePath, matchDataObjects, isGlobal, dynamodb):

    # Compile all of the matchDataObjects into a list of ResultCompilers that correspond to pathIDs
    pathIDUpdates = {}
    for matchData in matchDataObjects:
        idsToUpdate = getPathIDsToUpdate(matchData, basePath)

        for id in idsToUpdate:

            #Add new ids to update
            if id not in pathIDUpdates:
                if isGlobal:
                    pathIDUpdates[id] = GlobalResultCompiler()
                else:
                    pathIDUpdates[id] = PlayerResultCompiler()
            
            pathIDUpdates[id].handle_battle_result(matchData)

    def updatePath(pathID, resultCompiler, dynamodb):
        try:
            dynamodb.update_item(
                TableName=BRAWL_TRIE_TABLE,
                Key={
                    "pathID": {"S": pathID},
                    "datetime": {"S": "temp"}
                },
                ConditionExpression="attribute_exists(pathID)",
                UpdateExpression="""
                    SET
                        resultCompiler.player_result_data.wins = if_not_exists(resultCompiler.player_result_data.wins, :zero) + :wins,
                        resultCompiler.player_result_data.losses = if_not_exists(resultCompiler.player_result_data.losses, :zero) + :losses,
                        resultCompiler.player_result_data.draws = if_not_exists(resultCompiler.player_result_data.draws, :zero) + :draws,
                        resultCompiler.player_result_data.potential_total = if_not_exists(resultCompiler.player_result_data.potential_total, :zero) + :ptotal,

                        resultCompiler.player_star_data.wins = if_not_exists(resultCompiler.player_star_data.wins, :zero) + :swins,
                        resultCompiler.player_star_data.losses = if_not_exists(resultCompiler.player_star_data.losses, :zero) + :slosses,
                        resultCompiler.player_star_data.draws = if_not_exists(resultCompiler.player_star_data.draws, :zero) + :sdraws,
                        resultCompiler.player_star_data.potential_total = if_not_exists(resultCompiler.player_star_data.potential_total, :zero) + :sptotal,

                        resultCompiler.player_trophy_change = if_not_exists(resultCompiler.player_trophy_change, :zero) + :trophy
                """,
                ExpressionAttributeValues={
                    ":wins": {"N": str(resultCompiler.player_result_data.wins)},
                    ":losses": {"N": str(resultCompiler.player_result_data.losses)},
                    ":draws": {"N": str(resultCompiler.player_result_data.draws)},
                    ":ptotal": {"N": str(resultCompiler.player_result_data.potential_total)},
                    ":swins": {"N": str(resultCompiler.player_star_data.wins)},
                    ":slosses": {"N": str(resultCompiler.player_star_data.losses)},
                    ":sdraws": {"N": str(resultCompiler.player_star_data.draws)},
                    ":sptotal": {"N": str(resultCompiler.player_star_data.potential_total)},
                    ":trophy": {"N": str(resultCompiler.player_trophy_change)},
                    ":zero": {"N": "0"}
                }
            )
            return True
        except Exception as e:
            print(e)
            print()
            return False

    def addPath(pathID, childrenPathIDs, dynamodb):
        # Just add the base object

        baseCompiler = PlayerResultCompiler().to_dict()
        newItem = getTrieNodeItem(baseCompiler, pathID, childrenPathIDs)

        dynamodb.put_item(
            TableName=BRAWL_TRIE_TABLE,
            Item=prepareItem(newItem)
        )

        # Reference what the parent should be
        def getParentPath(pathID):
            if '$' not in pathID:
                return pathID
            return pathID[:pathID.rfind('$')]

        parentPath = getParentPath(pathID)


        # Attempt to add this as a child to the parent
        # If it fails, the parent doesn't exist, so create the parent
        def addChildPathID(parentPathID, childPathID, dynamodb):
            try:
                response = dynamodb.update_item(
                    TableName=BRAWL_TRIE_TABLE,
                    Key={"pathID": {"S": parentPathID}, "datetime": {"S": "temp"}},
                    UpdateExpression="""
                        SET childrenPathIDs = list_append(if_not_exists(childrenPathIDs, :empty_list), :new_child)
                    """,
                    ConditionExpression="attribute_exists(pathID)",
                    ExpressionAttributeValues={
                        ":new_child": {"L": [{"S": childPathID}]},
                        ":empty_list": {"L": []},
                    },
                    ReturnValues="UPDATED_NEW"
                )
                print("Successfully updated:", response["Attributes"]["childrenPathIDs"])
                return True
            except Exception as e:
                return False

        if addChildPathID(parentPathID=parentPath, childPathID=pathID, dynamodb=dynamodb):
            pass
        else:
            # Recursively create parent with this as child
            addPath(parentPath, [pathID], dynamodb)

    for pathID, resultCompiler in pathIDUpdates.items():
        print(pathID)
        # print(resultCompiler)
        print()
        if updatePath(pathID, resultCompiler, dynamodb):
            print("Success")
            pass
        else:
            print("Failed, adding")
            addPath(pathID, [], dynamodb)
            newUpdateResult = updatePath(pathID, resultCompiler, dynamodb)
            print("Update result ", newUpdateResult)
    
    
def getPathIDsToUpdate(matchData, basePath=""):
    result = []
    def addPathID(path):
        result.append(basePath + path)

    #type.brawler.mode.map:
    addPathID(f"$brawlerModeMap${matchData.type}")
    addPathID(f"$brawlerModeMap${matchData.type}${matchData.brawler}")
    addPathID(f"$brawlerModeMap${matchData.type}${matchData.brawler}${matchData.mode}")
    addPathID(f"$brawlerModeMap${matchData.type}${matchData.brawler}${matchData.mode}${matchData.map}")

    #type.mode.map.brawler:
    addPathID(f"$modeMapBrawler${matchData.type}")
    addPathID(f"$modeMapBrawler${matchData.type}${matchData.mode}")
    addPathID(f"$modeMapBrawler${matchData.type}${matchData.mode}${matchData.map}")
    addPathID(f"$modeMapBrawler${matchData.type}${matchData.mode}${matchData.map}${matchData.brawler}")

    #type.mode.brawler
    # addPathID(f"${type}")
    # addPathID(f"${type}${mode}")

    #This IS needed: find out what brawlers you have played soloShowdown with
    addPathID(f"$modeBrawler${matchData.type}${matchData.mode}${matchData.brawler}")

    return result

