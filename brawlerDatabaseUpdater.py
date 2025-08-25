import boto3
from DatabaseUtility.brawlerListUtility import cacheBrawlerList

if __name__ == "__main__":
    
    DYNAMODB_REGION = 'us-west-1'
    dynamodb = boto3.client("dynamodb", region_name=DYNAMODB_REGION)

    cacheBrawlerList(dynamodb)