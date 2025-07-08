import json

from CompilerStructuresModule.CompilerStructures.globalResultCompiler import GlobalResultCompiler
from CompilerStructuresModule.CompilerStructures.playerResultCompiler import PlayerResultCompiler
from DatabaseUtility.itemUtility import batch_get_all_items, batchWriteToDynamoDB, prepareItem
from DatabaseUtility.playerUtility import getPlayerStatsObject

from boto3.dynamodb.types import TypeDeserializer

BRAWL_TRIE_TABLE = "BrawlTrieStorage"

# Saving:
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
        "filterID": "temp",
        "childrenPathIDs": childrenPathIDs,
        "resultCompiler": resultCompilerJSON
    }

    return item

# Updating with game data:
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
            # Base ADD update expression for fixed fields
            update_expr_parts = [
                "resultCompiler.player_result_data.wins :wins",
                "resultCompiler.player_result_data.losses :losses",
                "resultCompiler.player_result_data.draws :draws",
                "resultCompiler.player_result_data.potential_total :ptotal",

                "resultCompiler.player_star_data.wins :swins",
                "resultCompiler.player_star_data.losses :slosses",
                "resultCompiler.player_star_data.draws :sdraws",
                "resultCompiler.player_star_data.potential_total :sptotal"
            ]

            # ExpressionAttributeValues for the fixed fields
            expr_attr_values = {
                ":wins": {"N": str(resultCompiler.player_result_data.wins)},
                ":losses": {"N": str(resultCompiler.player_result_data.losses)},
                ":draws": {"N": str(resultCompiler.player_result_data.draws)},
                ":ptotal": {"N": str(resultCompiler.player_result_data.potential_total)},
                ":swins": {"N": str(resultCompiler.player_star_data.wins)},
                ":slosses": {"N": str(resultCompiler.player_star_data.losses)},
                ":sdraws": {"N": str(resultCompiler.player_star_data.draws)},
                ":sptotal": {"N": str(resultCompiler.player_star_data.potential_total)}
            }

            if not isGlobal:

                update_expr_parts.append("resultCompiler.player_trophy_change :trophy")
                expr_attr_values[":trophy"] = {"N": str(resultCompiler.player_trophy_change)}

                expr_attr_names = {}

                # Add merging for resultCompiler.duration_frequencies.frequencies
                frequencies_to_add = resultCompiler.duration_frequencies.frequencies

                for key, delta in frequencies_to_add.items():
                    name_key = f"#k{key}"      # for attribute name placeholder
                    value_key = f":inc{key}"   # for value placeholder

                    expr_attr_names[name_key] = key
                    expr_attr_values[value_key] = {"N": str(delta)}
                    update_expr_parts.append(f"resultCompiler.duration_frequencies.frequencies.{name_key} {value_key}")

                update_expr = "ADD " + ",\n    ".join(update_expr_parts)

            # print("UpdateExpression:")
            # print(update_expr)
            # print("ExpressionAttributeValues:")
            # print(expr_attr_values)
            # print("ExpressionAttributeNames:")
            # print(expr_attr_names)

            # Perform the update
            dynamodb.update_item(
                TableName=BRAWL_TRIE_TABLE,
                Key={
                    "pathID": {"S": pathID},
                    "filterID": {"S": "temp"}
                },
                ConditionExpression="attribute_exists(pathID)",
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_attr_values,
                ExpressionAttributeNames=expr_attr_names if expr_attr_names else None
            )

            return True

        except Exception as e:
            print("Error during update:", e)
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
                    Key={"pathID": {"S": parentPathID}, "filterID": {"S": "temp"}},
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

# Fetching:
def fetchTrieData(basePath, type, mode, map, brawler, targetAttribute, dynamodb):
    # See TrieStorageNew.md -> ## Fetching
    deserializer = TypeDeserializer()

    PROJECTION_EXPRESSION = "pathID, resultCompiler"

    if type is not None:

        # For these, get the object with the corresponding id
        # For the first outcome, this would be modeMapBrawler$type$mode$map
        # Get the children of this object and return them
        def fetchChildrenNodes(pathID, dynamodb):
            response = dynamodb.get_item(
                TableName=BRAWL_TRIE_TABLE,
                Key={"pathID": {"S": pathID}, "filterID": {"S": "temp"}}
            )

            if 'Item' not in response:
                return []
            

            childrenPathIDs = deserializer.deserialize(response['Item']["childrenPathIDs"])

            childrenPathIDKeys = [{"pathID": {"S": childPathID}, "filterID": {"S": "temp"}} for childPathID in childrenPathIDs]
            childrenItems = batch_get_all_items(BRAWL_TRIE_TABLE, childrenPathIDKeys, dynamodb, PROJECTION_EXPRESSION)

            deserializedResult = [{k: deserializer.deserialize(v) for k, v in childItem.items()} for childItem in childrenItems]
            return deserializedResult

        def getPathForFetchWithTypeAsParameter(targetAttribute, type, mode, map, brawler):
            if targetAttribute == "brawler":
                if map is not None:
                    #typeModeMapBrawler type + mode + map -> brawlers
                    return f"modeMapBrawler${type}${mode}${map}"
                elif mode is not None:
                    #typeModeBrawler type + mode -> brawlers
                    return f"modeBrawler${type}${mode}"
                else:
                    #typeBrawlerModeMap type -> brawlers
                    return f"brawlerModeMap${type}"
            elif targetAttribute == "mode":
                if brawler is not None:
                    #typeBrawlerModeMap type + brawler -> modes
                    return f"brawlerModeMap${type}${brawler}"
                else:
                    #typeModeMapBrawler type -> modes
                    return f"modeMapBrawler${type}"
            elif targetAttribute == "map":
                if brawler is not None:
                    #typeBrawlerModeMap type + brawler + mode -> maps
                    return f"brawlerModeMap${type}${brawler}${mode}"
                else:
                    #typeModeMapBrawler type + mode -> maps
                    return f"modeMapBrawler${type}${mode}"
            else:
                raise Exception("Invalid targetAttribute!")

        fullPath = f"{basePath}${getPathForFetchWithTypeAsParameter(targetAttribute, type, mode, map, brawler)}"
        return fetchChildrenNodes(fullPath, dynamodb)

    else:
        # targetAttribute == "type" and type is None
        # This can probably be merged with the above large targetAttribute elif blocks by adding elif targetAttribute == "type"

        # For these, requests are harder since type is always the root of the path
        # Somehow get all types, for now, this can be stored in a variable potentialTypes = ["regular", "ranked"]
        # For each type, fetch it along with the parameters
        # These objects are what are returned

        # For now, there's only 2 types
        # If there are custom types in the future, somehow get all this player's types from $mapType$.children
        # For the first outcome, fetch baseTag$typeBrawlerModeMap$.children to get all of the types

        def getPotentialTypes():
            return ["regular", "ranked"]

        def getPathForFetchWithTypeAsTarget(type, mode, map, brawler):
            if brawler is not None:
                if map is not None:
                    return f"brawlerModeMap${type}${brawler}${mode}${map}"
                elif mode is not None:
                    return f"brawlerModeMap${type}${brawler}${mode}"
                else:
                    return f"brawlerModeMap${type}${brawler}"
            elif mode is not None:
                if map is not None:
                    return f"modeMapBrawler${type}${mode}${map}"
                else:
                    return f"modeMapBrawler${type}${mode}"
            else:
                raise Exception("Brawler and mode cannot both be None!")

        result = []

        for potentialType in getPotentialTypes():

            fullPath = f"{basePath}${getPathForFetchWithTypeAsTarget(potentialType, mode, map, brawler)}"
            response = dynamodb.get_item(
                TableName=BRAWL_TRIE_TABLE,
                Key={"pathID": {"S": fullPath}, "filterID": {"S": "temp"}},
                ProjectionExpression=PROJECTION_EXPRESSION
            )

            if 'Item' in response:
                result.append({k: deserializer.deserialize(v) for k, v in response['Item'].items()})
        
        return result
