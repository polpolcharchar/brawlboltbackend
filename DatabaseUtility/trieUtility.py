import math

from CompilerStructuresModule.CompilerStructures.matchData import MatchData
from CompilerStructuresModule.CompilerStructures.resultCompiler import ResultCompiler
from DatabaseUtility.itemUtility import batch_get_all_items, deserializeDynamoDbItem, prepareItem
from DatabaseUtility.modeToMapOverrideUtility import getMode

BRAWL_TRIE_TABLE = "BrawlStarsTrieData2"
    
def getTrieNodeItem(resultCompilerJSON, pathID, filterID, childrenPathIDs):
    item = {
        "pathID": pathID,
        "filterID": filterID,
        "resultCompiler": resultCompilerJSON
    }

    if childrenPathIDs:
        item["childrenPathIDs"] = childrenPathIDs

    return item

# Updating with game data:
def updateDatabaseTrie(basePath, matchDataObjects, filterID, dynamodb, isGlobal, skipToAddImmediately=False):

    pathIDUpdates = getCompilersToUpdate(matchDataObjects, basePath, isGlobal)

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

            # if not isGlobal:

            update_expr_parts.append("resultCompiler.player_trophy_change :trophy")
            expr_attr_values[":trophy"] = {"N": str(resultCompiler.player_trophy_change)}

            expr_attr_names = {}

            # Add merging for resultCompiler.duration_frequencies.frequencies
            frequencies_to_add = resultCompiler.duration_frequencies.frequencies

            for key, delta in frequencies_to_add.items():

                sanitizedKey = key.replace("-", "_")

                name_key = f"#k{sanitizedKey}"      # for attribute name placeholder
                value_key = f":inc{sanitizedKey}"   # for value placeholder

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

            if len(expr_attr_names) > 0:
                # Perform the update
                dynamodb.update_item(
                    TableName=BRAWL_TRIE_TABLE,
                    Key={
                        "pathID": {"S": pathID},
                        "filterID": {"S": filterID}
                    },
                    # ConditionExpression="attribute_exists(pathID)",
                    UpdateExpression=update_expr,
                    ExpressionAttributeValues=expr_attr_values,
                    ExpressionAttributeNames=expr_attr_names
                )
            else:
                # Perform the update
                dynamodb.update_item(
                    TableName=BRAWL_TRIE_TABLE,
                    Key={
                        "pathID": {"S": pathID},
                        "filterID": {"S": filterID}
                    },
                    # ConditionExpression="attribute_exists(pathID)",
                    UpdateExpression=update_expr,
                    ExpressionAttributeValues=expr_attr_values,
                )

            return True

        except Exception as e:
            # print("Error during update:")
            # print(e)
            # print(pathID)
            # print(filterID)
            # print(update_expr)
            # print(expr_attr_names)
            # print(expr_attr_values)
            return False

    def addPath(pathID, childrenPathIDs, dynamodb):

        baseCompiler = ResultCompiler().to_dict()

        # Prevent Self Reference
        if pathID in childrenPathIDs:
            childrenPathIDs = set(childrenPathIDs)
            childrenPathIDs.discard(pathID)

        newItem = getTrieNodeItem(baseCompiler, pathID, filterID, childrenPathIDs)

        dynamodb.put_item(
            TableName=BRAWL_TRIE_TABLE,
            Item=prepareItem(newItem)
        )

        # Reference what the parent should be
        def getParentPath(pathID):
            if '$' not in pathID:
                return None
            return pathID[:pathID.rfind('$')]

        parentPath = getParentPath(pathID)

        if not parentPath:
            return


        # Attempt to add this as a child to the parent
        # If it fails, the parent doesn't exist, so create the parent
        # If childrenPathIDs doesn't exist, it is created (i think)
        def addChildPathID(parentPathID, childPathID, dynamodb):
            try:
                response = dynamodb.update_item(
                    TableName=BRAWL_TRIE_TABLE,
                    Key={"pathID": {"S": parentPathID}, "filterID": {"S": filterID}},
                    UpdateExpression="ADD childrenPathIDs :new_child",
                    ConditionExpression="attribute_exists(pathID)",
                    ExpressionAttributeValues={
                        ":new_child": {"SS": [childPathID]},
                    },
                    ReturnValues="UPDATED_NEW"
                )
                # print("Successfully updated:", response["Attributes"]["childrenPathIDs"])
                return True
            except Exception as e:
                # print("Error add child path: ")
                # print(e)
                # print(parentPathID)
                # print(childPathID)
                return False

        # print("Attempting to add this as a child to parent")
        if addChildPathID(parentPathID=parentPath, childPathID=pathID, dynamodb=dynamodb):
            pass
        else:
            addPath(parentPath, {pathID}, dynamodb)

    # print(f"{len(pathIDUpdates)} paths to update")

    # startTime = time.time()
    # print("Starting update...")

    count = 0

    # print(f"updating {len(pathIDUpdates)} paths, ", end="")
    for pathID, resultCompiler in pathIDUpdates.items():
        if updatePath(pathID, resultCompiler, dynamodb) and not skipToAddImmediately:
            pass
        else:
            addPath(pathID, set(), dynamodb)
            newUpdateResult = updatePath(pathID, resultCompiler, dynamodb)

            if not newUpdateResult:
                print()
                print("Failed to update after adding, this is a HUGE problem!")
                print()
        
        count += 1
    
    # endTime = time.time()
    # print(f"Update completed in {endTime - startTime:.2f} seconds.")
    # if endTime - startTime > 0:
        # Print the rate of paths updated per second
        # print(f"Updated {count} paths at a rate of {count / (endTime - startTime):.2f} paths/second.")
    
def getPathIDsToUpdate(matchData, basePath, isGlobal):
    result = []
    def addPathID(path):
        result.append(basePath + path)

    addPathID("")

    #type.mode.brawler
    # addPathID(f"${type}")
    # addPathID(f"${type}${mode}")
    #This IS needed: find out what brawlers you have played soloShowdown with
    addPathID(f"$modeBrawler${matchData.type}${matchData.mode}${matchData.brawler}")

    if not isGlobal:
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
    else:

        # Extension of modeBrawler
        # These are specifically global because they already exist in players' modemapbrawler
        addPathID(f"$modeBrawler${matchData.type}")
        addPathID(f"$modeBrawler${matchData.type}${matchData.mode}")

        # Brawler
        addPathID(f"$brawlerMode${matchData.type}")
        addPathID(f"$brawlerMode${matchData.type}${matchData.brawler}")
        addPathID(f"$brawlerMode${matchData.type}${matchData.brawler}${matchData.mode}")

    return result

def getCompilersToUpdate(matchDataObjects, basePath, isGlobal):
    # Compile all of the matchDataObjects into a list of ResultCompilers that correspond to pathIDs

    pathIDUpdates = {}
    for matchData in matchDataObjects:
        idsToUpdate = getPathIDsToUpdate(matchData, basePath, isGlobal)

        for id in idsToUpdate:
            if id not in pathIDUpdates:
                pathIDUpdates[id] = ResultCompiler()
            
            pathIDUpdates[id].handle_battle_result(matchData)
    
    return pathIDUpdates

def getMatchDataObjectsFromGame(game, playerTag, includeAllPlayers):

    def getType(game):
        return "ranked" if game['battle']['type'] == "soloRanked" else "regular"
    def isShowdownVictory(game):
        if not 'rank' in game['battle']:
            raise KeyError("This isn't a showdown game!")
        
        if 'players' in game['battle']:
            return game['battle']['rank'] <= math.floor(len(game['battle']['players']) / 2)
        else:
            return game['battle']['rank'] <= math.floor(len(game['battle']['teams']) / 2)
    def getTrophyChange(game, playerTag):
            if 'trophyChange' in game['battle']:
                return game['battle']['trophyChange']
            
            if 'players' in game['battle'] and 'brawlers' in game['battle']['players'][0]:
                for player in game['battle']['players']:
                    if player['tag'] == playerTag:
                        total = 0
                        for brawler in player['brawlers']:
                            total += brawler['trophyChange']
                        return total
            
            return 0#change this!!
    def getWinningTeamIndex(game, playerTag):
        result = 1 if (game['battle']['result'] == "victory") else 0
        for player in game['battle']['teams'][0]:
            if player['tag'] == playerTag:
                return 1 - result
        return result

    def getMatchDataFromShowdown(game, playerTag):
        def getShowdownTeamPlayers(game, playerTag):
            if 'players' in game['battle']:
                for player in game['battle']['players']:
                    if player['tag'] == playerTag:
                        return [player]
            elif 'teams' in game['battle']:
                # Find the team that this player is on
                team_index = -1
                for i in range(len(game['battle']['teams'])):
                    for j in range(len(game['battle']['teams'][i])):
                        if game['battle']['teams'][i][j]['tag'] == playerTag:
                            team_index = i

                if team_index == -1:
                    print("Unable to find team!")
                    return []

                return game['battle']['teams'][team_index]
            else:
                print("Showdown has no players or teams!")
                return []

        # Collect Variables:
        is_star_player = game['battle']['rank'] == 1
        result_type = "wins" if isShowdownVictory(game) else "losses"

        playersOnThisTeam = getShowdownTeamPlayers(game, playerTag)

        result = []
        for player in playersOnThisTeam:
            if includeAllPlayers or player['tag'] == playerTag:
                result.append(MatchData(
                    game['event']['map'],
                    getMode(game),
                    player['brawler']['name'],
                    result_type,
                    is_star_player,
                    True,
                    None,
                    getTrophyChange(game, player['tag']),
                    getType(game)
                ))
        
        return result

    def getMatchDataFromDuels(game, playerTag):
        result = []

        for player in game['battle']['players']:
            result_type = (
                "draws"
                if game['battle']['result'] == "draw"
                else ("wins" if playerTag == player['tag'] and game['battle']['result'] == "victory" or playerTag != player['tag'] and game['battle']['result'] == "defeat" else "losses")
            )

            if includeAllPlayers or player['tag'] == playerTag:
                for brawler in player['brawlers']:
                    result.append(MatchData(
                            game['event']['map'],
                            getMode(game),
                            brawler['name'],
                            result_type,
                            False,
                            False,
                            game['battle']['duration'],
                            getTrophyChange(game, player['tag']),
                            getType(game)
                        )
                    )
            
        return result

    def getMatchDataFromRegular(game, playerTag):
        result = []

        winningTeamIndex = getWinningTeamIndex(game, playerTag)

        for teamIndex in range(2):
            for player in game['battle']['teams'][teamIndex]:

                is_star_player = player['tag'] == game['battle']['starPlayer']['tag'] if game['battle']['starPlayer'] else False
                result_type = (
                    "draws"
                    if game['battle']['result'] == "draw"
                    else ("wins" if winningTeamIndex == teamIndex else "losses")
                )

                if includeAllPlayers or player['tag'] == playerTag:
                    result.append(MatchData(
                            game['event']['map'],
                            getMode(game),
                            player['brawler']['name'],
                            result_type,
                            is_star_player,
                            game['battle']['starPlayer'] is not None,
                            game['battle']['duration'],
                            getTrophyChange(game, player['tag']),
                            getType(game)
                        )
                    )

        return result

    if 'rank' in game['battle']:
        return getMatchDataFromShowdown(game, playerTag)
    elif getMode(game) == "duels":
        return getMatchDataFromDuels(game, playerTag)
    elif 'teams' not in game['battle']:
        return []
    elif len(game['battle']['teams']) != 2:
        return []
    else:
        return getMatchDataFromRegular(game, playerTag)


# Fetching:
def fetchTrieData(basePath, filterID, type, mode, map, brawler, targetAttribute, isGlobal, dynamodb):    

    if targetAttribute != "type" and targetAttribute is not None:

        if type is None:
            raise Exception("Type must be provided if targetAttribute is not 'type'!")

        # For these, get the object with the corresponding id
        # For the first outcome, this would be modeMapBrawler$type$mode$map
        # Get the children of this object and return them

        def fetchChildrenPaths(pathID, dynamodb):
            response = dynamodb.get_item(
                TableName=BRAWL_TRIE_TABLE,
                Key={"pathID": {"S": pathID}, "filterID": {"S": filterID}},
                ProjectionExpression="childrenPathIDs"
            )

            if 'Item' not in response:
                return []
            
            if 'childrenPathIDs' not in response['Item']:
                return []
            
            childrenPathIDs = deserializeDynamoDbItem(response['Item'])['childrenPathIDs']

            return childrenPathIDs

        def fetchChildrenNodes(pathID, dynamodb):
            childrenPathIDs = fetchChildrenPaths(pathID, dynamodb)

            childrenPathIDKeys = [{"pathID": {"S": childPathID}, "filterID": {"S": filterID}} for childPathID in childrenPathIDs]
            childrenItems = batch_get_all_items(BRAWL_TRIE_TABLE, childrenPathIDKeys, dynamodb, projection_expression="pathID, resultCompiler")

            deserializedResult = [deserializeDynamoDbItem(childItem) for childItem in childrenItems]
            return deserializedResult

        def getPathForFetchWithTypeAsParameter(targetAttribute, type, mode, map, brawler, isGlobal):
            if targetAttribute == "brawler":
                if map is not None:
                    #typeModeMapBrawler type + mode + map -> brawlers
                    return f"modeMapBrawler${type}${mode}${map}"
                elif mode is not None:
                    #typeModeBrawler type + mode -> brawlers
                    return f"modeBrawler${type}${mode}"
                else:
                    #typeBrawlerModeMap type -> brawlers
                    if isGlobal:
                        return f"brawlerMode${type}"
                    else:
                        return f"brawlerModeMap${type}"
            elif targetAttribute == "mode":
                if brawler is not None:
                    #typeBrawlerModeMap type + brawler -> modes
                    if isGlobal:
                        return f"brawlerMode${type}${brawler}"
                    else:
                        return f"brawlerModeMap${type}${brawler}"
                else:
                    #typeModeMapBrawler type -> modes
                    if isGlobal:
                        return f"modeBrawler${type}"
                    else:
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

        fullPath = f"{basePath}${getPathForFetchWithTypeAsParameter(targetAttribute, type, mode, map, brawler, isGlobal)}"

        potentialMaps = []
        if not isGlobal and mode is not None:
            mapParentPath = f"{basePath}${getPathForFetchWithTypeAsParameter('map', type, mode, map, brawler, isGlobal)}"
            mapPaths = fetchChildrenPaths(mapParentPath, dynamodb)
            potentialMaps = [mp.split('$')[-1] for mp in mapPaths]

        return {
            "trieData": fetchChildrenNodes(fullPath, dynamodb),
            "potentialMaps": potentialMaps
        }

    else:
        # targetAttribute == "type"
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

        def getPathForFetchWithTypeAsTarget(type, mode, map, brawler, isGlobal):
            if brawler is not None:
                if map is not None:
                    return f"brawlerModeMap${type}${brawler}${mode}${map}"
                elif mode is not None:
                    if isGlobal:
                        return f"modeBrawler${type}${mode}${brawler}"
                    else:
                        return f"brawlerModeMap${type}${brawler}${mode}"
                else:
                    if isGlobal:
                        return f"brawlerMode${type}${brawler}"
                    else:
                        return f"brawlerModeMap${type}${brawler}"
            elif mode is not None:
                if map is not None:
                    return f"modeMapBrawler${type}${mode}${map}"
                else:
                    if isGlobal:
                        return f"modeBrawler${type}${mode}"
                    else:
                        return f"modeMapBrawler${type}${mode}"
            else:
                return f"modeBrawler${type}" # Any of the base maps could go here, because they all begin with type
                # Need to implement a way to actually assign the right stats here when transitioning from old format

        if targetAttribute is None:
            path = f"{basePath}${getPathForFetchWithTypeAsTarget(type, mode, map, brawler, isGlobal)}"

            response = dynamodb.get_item(
                TableName=BRAWL_TRIE_TABLE,
                Key={"pathID": {"S": path}, "filterID": {"S": filterID}},
                ProjectionExpression="pathID, resultCompiler"
            )

            if 'Item' not in response:
                return {
                    "trieData": [],
                    "potentialMaps": []
                }
            
            deserializedItem = deserializeDynamoDbItem(response['Item'])

            return {
                "trieData": [deserializedItem],
                "potentialMaps": []
            }

        result = []
        potentialMapsSet = set()

        # If there is mode but no map, include children maps
        childrenPathIDsNeededForMaps = mode is not None and map is None
        conditionalProjectionExpression = "pathID, resultCompiler, childrenPathIDs" if childrenPathIDsNeededForMaps else "pathID, resultCompiler"

        for potentialType in getPotentialTypes():

            fullPath = f"{basePath}${getPathForFetchWithTypeAsTarget(potentialType, mode, map, brawler, isGlobal)}"
            response = dynamodb.get_item(
                TableName=BRAWL_TRIE_TABLE,
                Key={"pathID": {"S": fullPath}, "filterID": {"S": filterID}},
                ProjectionExpression=conditionalProjectionExpression
            )

            if 'Item' in response:
                deserializedItem = deserializeDynamoDbItem(response['Item'])

                result.append(deserializedItem)

                if "childrenPathIDs" in deserializedItem:
                    for childPathID in deserializedItem["childrenPathIDs"]:
                        potentialMapsSet.add(childPathID.split('$')[-1])
        
        return {
            "trieData": result,
            "potentialMaps": list(potentialMapsSet)
        }

def fetchRecentTrieData(basePath, numItems, isGlobal, type, mode, map, brawler, targetAttribute, dynamodb):
    filterIDs = []

    try:
        response = dynamodb.query(
            TableName=BRAWL_TRIE_TABLE,
            KeyConditionExpression="pathID = :pathID",
            ExpressionAttributeValues={
                ":pathID": {"S": basePath}
            },
            ScanIndexForward=False,
            Limit=numItems,
            ProjectionExpression="filterID"
        )

        if 'Items' not in response or len(response['Items']) == 0:
            return []

        for item in response['Items']:
            filterID = item['filterID']['S']
            filterIDs.append(filterID)

    except Exception as e:
        print(f"Error fetching trie data: {e}")
        return []

    fetchResults = []

    for filterID in filterIDs:
        fetchResult = fetchTrieData(
            basePath, filterID, type, mode, map, brawler, targetAttribute, isGlobal, dynamodb
        )
        fetchResult['datetime'] = filterID
        fetchResults.append(fetchResult)
    
    return fetchResults

# Removing
def removeTrieNodeAndChildren(pathID, filterID, dynamodb):
    try:
        # Fetch the node
        response = dynamodb.get_item(
            TableName=BRAWL_TRIE_TABLE,
            Key={"pathID": {"S": pathID}, "filterID": {"S": filterID}}
        )

        if 'Item' not in response:
            print(f"Node not found: {pathID}")
            return

        item = response['Item']

        # Recursively delete children if present
        if 'childrenPathIDs' in item and 'SS' in item['childrenPathIDs']:
            for childPathID in item['childrenPathIDs']['SS']:
                if childPathID != pathID:
                    removeTrieNodeAndChildren(childPathID, filterID, dynamodb)

        # Delete this node
        dynamodb.delete_item(
            TableName=BRAWL_TRIE_TABLE,
            Key={"pathID": {"S": pathID}, "filterID": {"S": filterID}}
        )
        print(f"Deleted: {pathID}")

    except Exception as e:
        print(f"Error deleting {pathID}: {e}")