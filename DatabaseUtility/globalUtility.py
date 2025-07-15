
# import json

# from DatabaseUtility.itemUtility import deserializeDynamoDbItem, fullyJSONifyData
# from brawlStats import BrawlStats

# globalDataTable = 'BrawlStarsGlobalData2'

# def getDeserializedGlobalStats(dynamodb):
#     deserialized = {}

#     statTypes = ["regularModeBrawler", "regularBrawler", "rankedModeBrawler", "rankedBrawler"]
#     for t in statTypes:
#         response = dynamodb.query(
#             TableName=globalDataTable,
#             KeyConditionExpression='statType = :statType',
#             ExpressionAttributeValues={
#                 ':statType': {'S': t},
#             },
#             ScanIndexForward=False,
#             Limit=1
#         )

#         #Assign these variables so they aren't redundant
#         if 'numGames' not in deserialized:
#             deserialized['numGames'] = response['Items'][0]['numGames']['N']
#             deserialized['datetime'] = response['Items'][0]['datetime']['S']
#             deserialized['hourRange'] = response['Items'][0]['hourRange']['N']

#         deserialized[t] = json.loads(response['Items'][0]['stats']['S'])
    
#     return deserialized
# def getSpecificGlobalStatOverTime(statTypeString, dynamodb, limit=20):
#     response = dynamodb.query(
#         TableName=globalDataTable,
#         KeyConditionExpression='statType = :statType',
#         ExpressionAttributeValues={
#             ':statType': {'S': statTypeString},
#         },
#         ScanIndexForward=False,
#         Limit=limit
#     )

#     if not response.get('Items'):
#         return None

#     # Specific global stats used to be stored using dynamodb structures, using convertToDynamodbFormat and deserializeDynamoDbItem
#     # Now, they are stored as stringified json
#     # This requires less storage and removes the need to deal with type Decimal being returned
#     # Change made on 7/6 - this else statement can be removed after a significant amount of time
#     return [
#             {key: (json.loads(value) if key == "stats" else value) for key, value in deserializeDynamoDbItem(item).items()}
#             if 'S' in item['stats']
#             else fullyJSONifyData(deserializeDynamoDbItem(item))
#             for item in response['Items']
#             ]

# def getGlobalStatsObject(datetime, dynamodb):
#     statTypeToBrawlStatNames = {
#         "regularModeBrawler": "regular_mode_brawler",
#         "regularBrawler": "regular_brawler",
#         "rankedModeBrawler": "ranked_mode_brawler",
#         "rankedBrawler": "ranked_brawler"
#     }

#     globalDataJSON = {}

#     for statType, brawlName in statTypeToBrawlStatNames.items():

#         response = dynamodb.get_item(
#             TableName=globalDataTable,
#             Key={"statType": {"S": statType}, "datetime": {"S": datetime}}
#         )

#         if 'Item' not in response:
#             print("Missing item!", statType)
#             return False
        
#         globalDataJSON[brawlName] = json.loads(response['Item']['stats']['S'])
    
#     return BrawlStats(True, globalDataJSON)

