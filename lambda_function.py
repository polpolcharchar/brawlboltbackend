import json
import boto3
from DatabaseUtility.itemUtility import decimalAndSetSerializer, deserializeDynamoDbItem
from DatabaseUtility.playerUtility import beginTrackingPlayer, compileUncachedStats, getPlayerInfo, updateStatsLastAccessed
from DatabaseUtility.trieUtility import BRAWL_TRIE_TABLE, fetchTrieData, fetchRecentTrieData

CORS_HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'OPTIONS,POST',
  'Access-Control-Allow-Headers': 'Content-Type',
}

# Initialize DynamoDB client
DYNAMODB_REGION = 'us-west-1'
dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

def lambda_handler(event, context):

    eventBody = json.loads(event['body'])

    if eventBody['type'] == "getRecentGlobalScanInfo":

        response = dynamodb.query(
            TableName=BRAWL_TRIE_TABLE,
            KeyConditionExpression="pathID = :pathID",
            ExpressionAttributeValues={
                ":pathID": {"S": "global"}
            },
            ScanIndexForward=False,
            Limit=1,
            ProjectionExpression="filterID, numGames, hourRange"
        )

        if len(response['Items']) == 0:
            return {
                'statusCode': 500,
                'body': json.dumps({'message': 'Failed to fetch global stats object.'}),
                'headers': CORS_HEADERS,
            }
        
        rootGlobalStatObject = response['Items'][0]
        deserializedItem = deserializeDynamoDbItem(rootGlobalStatObject)

        return {
            'statusCode': 200,
            'body': json.dumps(deserializedItem, default=lambda x: decimalAndSetSerializer(x)),
            'headers': CORS_HEADERS
        }

    elif eventBody['playerTag'] == "":
        return {
            'statusCode': 502,
            'body': json.dumps({'message': 'Invalid playerTag'}),
            'headers': CORS_HEADERS,
        }
    
    elif eventBody['type'] == 'getTrieData':

        requestedType = eventBody.get('requestType')
        requestedMap = eventBody.get('requestMap')
        requestedMode = eventBody.get('requestMode')
        requestedBrawler = eventBody.get('requestBrawler')

        targetAttribute = eventBody['targetAttribute']

        basePath = eventBody['playerTag']
        filterID = eventBody['filterID']

        isGlobal = eventBody['isGlobal']

        try:
            fetchResult = fetchTrieData(
                basePath=basePath,
                filterID=filterID,
                type=requestedType,
                mode=requestedMode,
                map=requestedMap,
                brawler=requestedBrawler,
                targetAttribute=targetAttribute,
                dynamodb=dynamodb,
                isGlobal=isGlobal
            )
        except Exception as e:
            return {
                'statusCode': 502,
                'body': json.dumps({'message': f'Error fetching trie data: {str(e)}'}),
                'headers': CORS_HEADERS,
            }

        return {
            'statusCode': 200,
            'body': json.dumps(fetchResult, default=lambda x: decimalAndSetSerializer(x)),
            'headers': CORS_HEADERS
        }

    elif eventBody['type'] == 'getRecentTrieData':
        requestedType = eventBody.get('requestType')
        requestedMap = eventBody.get('requestMap')
        requestedMode = eventBody.get('requestMode')
        requestedBrawler = eventBody.get('requestBrawler')

        targetAttribute = eventBody.get('targetAttribute')

        basePath = eventBody['playerTag']

        isGlobal = eventBody['isGlobal']

        numItems = min(int(eventBody.get('numItems', 1)), 20)

        fetchResult = fetchRecentTrieData(
            basePath=basePath,
            numItems=numItems,
            isGlobal=isGlobal,
            type=requestedType,
            mode=requestedMode,
            map=requestedMap,
            brawler=requestedBrawler,
            targetAttribute=targetAttribute,
            dynamodb=dynamodb
        )

        if fetchResult is None:
            return {
                'statusCode': 502,
                'body': json.dumps({'message': 'Error fetching trie data over time'}),
                'headers': CORS_HEADERS,
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps(fetchResult, default=lambda x: decimalAndSetSerializer(x)),
            'headers': CORS_HEADERS
        }
        
    elif eventBody['type'] == 'getPlayerInfo':
        #Standard request 1
        response = getPlayerInfo(eventBody['playerTag'], dynamodb)

        #Begin tracking this player
        if len(response['Items']) == 0:
            trackingResult = beginTrackingPlayer(eventBody['playerTag'], dynamodb)

            if not trackingResult:
                return {
                    'statusCode': 502,
                    'body': json.dumps({'message': "Tracking Initialization Failed: Player Doesn't Exist"}),
                    'headers': CORS_HEADERS,
                }
            
            # Always compile new games for new players
            compileUncachedStats(eventBody['playerTag'], dynamodb)

            response = getPlayerInfo(eventBody['playerTag'], dynamodb)

            # If it is still zero, return error:
            if len(response['Items']) == 0:
                return {
                    'statusCode': 502,
                    'body': json.dumps({'message': 'Error Adding Player'}),
                    'headers': CORS_HEADERS,
                }
        
        #Standard request 2
        updateStatsLastAccessed(eventBody['playerTag'], dynamodb)

        resultBody = {
            "playerInfo": {"name": response["Items"][0]["username"]["S"]},
        }

        return {
            'statusCode': 200,
            'body': json.dumps(resultBody),
            'headers': CORS_HEADERS
        }
    
    else:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Invalid request type'}),
            'headers': CORS_HEADERS,
        }