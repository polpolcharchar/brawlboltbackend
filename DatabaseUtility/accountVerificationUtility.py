import uuid
import time
from passlib.hash import pbkdf2_sha256
import random
import jwt
from DatabaseUtility.secretsUtility import getSecret
from apiUtility import getApiPlayerIconID, getApiProxyPlayerIconID

VERIFICATION_TABLE = "BrawlStarsAccountVerification"
PLAYER_INFO_TABLE = "BrawlStarsPlayersInfo"

VERIFICATION_STEPS_REQUIRED = 2
TOKEN_EXPIRY_SECONDS = 15 * 60

NUM_VERIFICATIONS_REQUIRED = 3

BRAWLER_ICON_IDS = [
    28000003,  # Shelly
    28000007,  # Nita
    28000004,  # Colt
    28000010,  # Bull
    28000005,  # Brock
    28000009,  # El Primo
    28000012,  # Barley
    28000013,  # Poco
    28000040,  # Rosa
]

# The icon value in the api likely updates every ~60 seconds
def getRandomIconID(idToExclude=None):
    if idToExclude is not None:
        valid_ids = [icon_id for icon_id in BRAWLER_ICON_IDS if icon_id != idToExclude]
        if not valid_ids:
            raise ValueError("No valid icon IDs available after excluding the current one.")
        return random.choice(valid_ids)
    return random.choice(BRAWLER_ICON_IDS)


def handleAccountVerificationRequest(eventBody, dynamodb):
    verificationType = eventBody.get("verificationRequestType")
    playerTag = eventBody.get("playerTag")

    if not playerTag or not verificationType:
        return {"error": "Missing tag or verificationRequestType"}

    playerTag = playerTag.upper().replace("O", "0")

    if verificationType == "initiate":
        return handleInitiateVerification(playerTag, dynamodb)
    elif verificationType == "verifyStep":
        return handleVerifyStep(playerTag, eventBody, dynamodb)
    # elif verificationType == "finalize":
    #     return handleFinalize(playerTag, eventBody, dynamodb)
    else:
        return {"error": "Invalid verificationRequestType"}

def handleInitiateVerification(playerTag, dynamodb):
    token = str(uuid.uuid4())
    iconID = getRandomIconID()
    timestamp = int(time.time())

    dynamodb.put_item(
        TableName=VERIFICATION_TABLE,
        Item={
            "playerTag": {"S": playerTag},
            "token": {"S": token},
            "iconIdToSet": {"N": str(iconID)},
            "verifiedSteps": {"N": "0"},
            "createdAt": {"N": str(timestamp)},
        }
    )

    return {
        "token": token,
        "iconIdToSet": iconID
    }

def handleVerifyStep(playerTag, eventBody, dynamodb):
    token = eventBody.get("token")
    if not token:
        return {"error": "Missing token"}

    response = dynamodb.get_item(
        TableName=VERIFICATION_TABLE,
        Key={"playerTag": {"S": playerTag}}
    )

    item = response.get("Item")
    if not item:
        return {"error": "Verification not found"}

    if item["token"]["S"] != token:
        return {"error": "Invalid token"}

    if int(time.time()) - int(item["createdAt"]["N"]) > TOKEN_EXPIRY_SECONDS:
        return {"error": "Token expired"}

    expectedIconID = int(item["iconIdToSet"]["N"])
    actualIconID = getApiProxyPlayerIconID(playerTag)

    if actualIconID != expectedIconID:
        return {"error": "Icon does not match"}

    numVerifiedSteps = int(item["verifiedSteps"]["N"]) + 1
    newIconID = getRandomIconID(idToExclude=expectedIconID)

    update_expr = "SET verifiedSteps = :vs, iconIdToSet = :newIcon"
    expr_values = {
        ":vs": {"N": str(numVerifiedSteps)},
        ":newIcon": {"N": str(newIconID)}
    }

    dynamodb.update_item(
        TableName=VERIFICATION_TABLE,
        Key={"playerTag": {"S": playerTag}},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values
    )

    if numVerifiedSteps >= NUM_VERIFICATIONS_REQUIRED:
        return {
            "verifiedSteps": numVerifiedSteps,
            "readyForPassword": True
        }
    else:
        return {
            "verifiedSteps": numVerifiedSteps,
            "newIconIdToSet": newIconID
        }

def handleFinalize(playerTag, eventBody, dynamodb):
    token = eventBody.get("token")
    password = eventBody.get("password")

    if not token or not password:
        return {"error": "Missing token or password"}

    response = dynamodb.get_item(
        TableName=VERIFICATION_TABLE,
        Key={"playerTag": {"S": playerTag}}
    )
    item = response.get("Item")

    if not item or item["token"]["S"] != token:
        return {"error": "Invalid or missing verification"}

    if int(item["verifiedSteps"]["N"]) < VERIFICATION_STEPS_REQUIRED:
        return {"error": "Not enough verification steps"}

    if int(time.time()) - int(item["createdAt"]["N"]) > TOKEN_EXPIRY_SECONDS:
        return {"error": "Token expired"}

    hashedPassword = pbkdf2_sha256.hash(password)

    # Store the password in the final table
    dynamodb.update_item(
        TableName=PLAYER_INFO_TABLE,
        Key={"playerTag": {"S": playerTag}},
        UpdateExpression="SET #pw = :pw",
        ExpressionAttributeNames={"#pw": "password"},
        ExpressionAttributeValues={":pw": {"S": hashedPassword}}
    )

    # Remove old verification item
    dynamodb.delete_item(
        TableName=VERIFICATION_TABLE,
        Key={"playerTag": {"S": playerTag}}
    )

    return {"success": True}


def handleLogin(playerTag, password, dynamodb):
    # Fetch user info from DynamoDB
    response = dynamodb.get_item(
        TableName=PLAYER_INFO_TABLE,
        Key={"playerTag": {"S": playerTag}},
        ProjectionExpression="#pw",
        ExpressionAttributeNames={"#pw": "password"}
    )

    item = response.get("Item")
    if not item or "password" not in item:
        return {"error": "Account not found or not verified"}

    hashedPassword = item["password"]["S"]

    # Verify password
    if not pbkdf2_sha256.verify(password, hashedPassword):
        return {"error": "Incorrect password"}

    # Create JWT token
    payload = {
        "playerTag": playerTag,
        "exp": int(time.time()) + 3600  # 1 hour expiration
    }

    token = jwt.encode(payload, getSecret("JWT_SECRET"), algorithm="HS256")

    return {
        "token": token
    }

def verifyToken(token):
    try:
        payload = jwt.decode(token, getSecret("JWT_SECRET"), algorithms=["HS256"])
        return payload["playerTag"]
    except jwt.ExpiredSignatureError:
        return None  # expired
    except jwt.InvalidTokenError:
        return None  # invalid