
from apiUtility import getApiBrawlersList


BRAWLER_LIST_TABLE = "BrawlStarsBrawlers"

# Intended to be run locally; use direct api access, not proxy
def cacheBrawlerList(dynamodb):
    rawBrawlerList = getApiBrawlersList()

    brawlerNameList = [brawler["name"] for brawler in rawBrawlerList]

    dynamodb.update_item(
        TableName=BRAWLER_LIST_TABLE,
        Key={
            "id": {"S": "main"},
        },
        UpdateExpression="SET brawlerNames = :brawlerNames",
        ExpressionAttributeValues={
            ":brawlerNames": {"SS": brawlerNameList},
        }
    )

def getCachedBrawlerList(dynamodb):
    response = dynamodb.get_item(
        TableName=BRAWLER_LIST_TABLE,
        Key={
            "id": {"S": "main"},
        },
    )

    if 'Item' not in response:
        return None

    item = response['Item']
    if 'brawlerNames' not in item:
        return None

    brawlerNames = item['brawlerNames'].get('SS', [])
    return brawlerNames