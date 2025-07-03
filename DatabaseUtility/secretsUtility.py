import json
import boto3

_secretsMap = None
def getSecret(secretName):
    def fetchAndAssignSecrets():
        global _secretsMap

        DYNAMODB_REGION = 'us-west-1'
        SECRETS_TABLE = "BrawlStarsSecrets"
        dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

        try:
            response = dynamodb.get_item(
                TableName=SECRETS_TABLE,
                Key={'id': {'S': 'allSecrets'}}
            )

            print("Response:")
            print(response)

            if 'Item' in response and 'jsonValues' in response['Item']:
                secretsJsonString = response['Item']['jsonValues']['S']
                print("Json:")
                print(secretsJsonString)
                parsedSecrets = json.loads(secretsJsonString)
                _secretsMap = parsedSecrets
            else:
                print("no item")
                _secretsMap = {}
        except Exception as e:
            print("Error---")
            print(e)
            _secretsMap = {}
    
    if _secretsMap is None:
        fetchAndAssignSecrets()

    print(_secretsMap)
    print(secretName)
    
    return _secretsMap[secretName]

