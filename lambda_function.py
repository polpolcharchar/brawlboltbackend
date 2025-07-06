import json
import boto3
from datetime import datetime
from DatabaseUtility.globalUtility import getDeserializedGlobalStats, getSpecificGlobalStatOverTime
from DatabaseUtility.playerUtility import beginTrackingPlayer, compileUncachedStats, getPlayerCompiledStatsJSON, getPlayerInfo, getPlayerRegularModeMapBrawlerJSON, updateStatsLastAccessed

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

    if eventBody['type'] == 'getGlobalDataOverTime':

        g = getSpecificGlobalStatOverTime(eventBody['statType'], dynamodb)

        if g is None:
            return {
                'statusCode': 502,
                'body': json.dumps({'message': 'Error Loading Global Stats'}),
                'headers': CORS_HEADERS,
            }

        return {
            'statusCode': 200,
            'body': json.dumps(g),
            'headers': CORS_HEADERS,
        }

    elif eventBody['type'] == 'getGlobalStats':
        g = getDeserializedGlobalStats(dynamodb)

        if g is None:
            return {
                'statusCode': 502,
                'body': json.dumps({'message': 'Error Loading Global Stats'}),
                'headers': CORS_HEADERS,
            }

        return {
            'statusCode': 200,
            'body': json.dumps(g),
            'headers': CORS_HEADERS,
        }

    elif eventBody['playerTag'] == "":
        return {
            'statusCode': 502,
            'body': json.dumps({'message': 'Invalid playerTag'}),
            'headers': CORS_HEADERS,
        }
    
    elif 'o' in eventBody['playerTag']:
        return {
            'statusCode': 502,
            'body': json.dumps({'message': 'Invalid playerTag: cannot contain the letter o'}),
            'headers': CORS_HEADERS,
        }

    elif eventBody['type'] == 'getBaseRegularModeMapBrawler':
        #Only include overall data from 1 level deeper in the stat map:
        resultBody = {
            "regularModeMapBrawler": {
                "stat_map": {key: {"overall": value["overall"]} for key, value in getPlayerRegularModeMapBrawlerJSON(eventBody["playerTag"], dynamodb)["stat_map"].items()}
            }
        }

        return {
            'statusCode': 200,
            'body': json.dumps(resultBody),
            'headers': CORS_HEADERS
        }

    else:
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

            #Standard request 3
            "playerStats": getPlayerCompiledStatsJSON(eventBody['playerTag'], dynamodb)
        }

        return {
            'statusCode': 200,
            'body': json.dumps(resultBody),
            'headers': CORS_HEADERS
        }