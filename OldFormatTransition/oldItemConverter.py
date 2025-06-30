import json
import boto3
import pickle

from stats import FrequencyCompiler, RecursiveStatCompiler

DYNAMODB_REGION = 'us-west-1'

def convertItemToNewFormat(item, binaryAttributeName):
    binary = item[binaryAttributeName]["B"]
    deserialized = pickle.loads(binary)

    #This can't be used with the other version of this function in utility, because RSC and FC are different classes now
    def fullyJSONifyData(d):
        if isinstance(d, RecursiveStatCompiler):
            return d.to_dict()
        elif isinstance(d, FrequencyCompiler):
            return d.to_dict()
        elif isinstance(d, dict):
            return {key: fullyJSONifyData(value) for key, value in d.items()}
        elif isinstance(d, list):
            return [fullyJSONifyData(item) for item in d]
        elif isinstance(d, set):
            return [fullyJSONifyData(item) for item in sorted(d)]
        elif isinstance(d, tuple):
            return tuple(fullyJSONifyData(item) for item in d)
        else:
            return d
    
    jsonData = fullyJSONifyData(deserialized)

    item[binaryAttributeName] = {"S": json.dumps(jsonData)}

    return item

def convertTableToNewFormat(fromTable, primaryKeyName, targetPrimaryKey, binaryAttributeName, toTable=None):
    dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

    def get_all_items_from_query(table_name, primary_key_name, target_primary_key):
        items = []
        last_evaluated_key = None

        while True:
            query_kwargs = {
                "TableName": table_name,
                "KeyConditionExpression": f"{primary_key_name} = :pk",
                "ExpressionAttributeValues": {
                    ":pk": {"S": target_primary_key}
                }
            }
            
            if last_evaluated_key:
                query_kwargs["ExclusiveStartKey"] = last_evaluated_key
            
            response = dynamodb.query(**query_kwargs)
            items.extend(response.get("Items", []))

            # Check if there are more items to fetch
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        return items

    items = get_all_items_from_query(fromTable, primaryKeyName, targetPrimaryKey)
    for item in items:
        newItem = convertItemToNewFormat(item, binaryAttributeName)

        putResponse = dynamodb.put_item(
            TableName=(fromTable if toTable is None else toTable),
            Item=newItem,
        )
    print(targetPrimaryKey + " moved " + str(len(items)) + " items")


#Convert Global:
# globalStatTypes = ["regularBrawler", "rankedBrawler", "regularModeBrawler", "rankedModeBrawler"]
# for statType in globalStatTypes:
#     print(statType)
#     convertTableToNewFormat("BrawlStarsGlobalData2", "statType", statType, "stats")

#Convert players:
# dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)
# def getAllPlayersInfo():

#     items = []
#     last_evaluated_key = None

#     while True:
#         scanArguments = {
#             "TableName": "BrawlStarsPlayersInfo",
#         }
        
#         if last_evaluated_key:
#             scanArguments["ExclusiveStartKey"] = last_evaluated_key
        
#         response = dynamodb.scan(**scanArguments)
#         items.extend(response.get("Items", []))

#         # Check if there are more items to fetch
#         last_evaluated_key = response.get("LastEvaluatedKey")
#         if not last_evaluated_key:
#             break
    
#     return items

# playersInfo = getAllPlayersInfo()
# print(playersInfo)

# for player in playersInfo:
#     tag = player["playerTag"]["S"]

#     convertTableToNewFormat("BrawlStarsPlayers", "playerTag", tag, "stats", "BrawlStarsPlayers2")

