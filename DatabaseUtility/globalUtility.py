
import json

from DatabaseUtility.itemUtility import deserializeDynamoDbItem


globalDataTable = 'BrawlStarsGlobalData2'

def getDeserializedGlobalStats(dynamodb):
    deserialized = {}

    statTypes = ["regularModeBrawler", "regularBrawler", "rankedModeBrawler", "rankedBrawler"]
    for t in statTypes:
        response = dynamodb.query(
            TableName=globalDataTable,
            KeyConditionExpression='statType = :statType',
            ExpressionAttributeValues={
                ':statType': {'S': t},
            },
            ScanIndexForward=False,
            Limit=1
        )

        #Assign these variables so they aren't redundant
        if 'numGames' not in deserialized:
            deserialized['numGames'] = response['Items'][0]['numGames']['N']
            deserialized['datetime'] = response['Items'][0]['datetime']['S']
            deserialized['hourRange'] = response['Items'][0]['hourRange']['N']

        deserialized[t] = json.loads(response['Items'][0]['stats']['S'])
    
    return deserialized
def getSpecificGlobalStatOverTime(statTypeString, dynamodb, limit=20):
    response = dynamodb.query(
        TableName=globalDataTable,
        KeyConditionExpression='statType = :statType',
        ExpressionAttributeValues={
            ':statType': {'S': statTypeString},
        },
        ScanIndexForward=False,
        Limit=limit
    )

    if not response.get('Items'):
        return None

    #use deserializeDynamoDbItem to deserialize the stats
    deserialized = [deserializeDynamoDbItem(item) for item in response['Items']]

    return deserialized

