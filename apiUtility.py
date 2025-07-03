import requests
from dotenv import load_dotenv
import os

load_dotenv()

API_PROXY_URL = os.getenv("BRAWL_STARS_API_PROXY_URL")
BRAWL_API_KEY = os.getenv("BRAWL_API_KEY")

# The Brawl Stars API requires you to associate an ip address with an API key
# Because of this, serverless Lambda functions cannot directly access the API
# ApiProxy functions access BrawlBolt's API proxy that mimics the API from a static IP
# Pure Api functions assume that the code is being run from the ip that is associated with the API key

def requestApiProxy(endpoint):

    body = {
        "endpoint": endpoint,
        "params": {}
    }

    try:
        response = requests.post(API_PROXY_URL, json=body)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return None

def getApiProxyPlayerInfo(playerTag):

    endpoint = "players/%23" + playerTag

    return requestApiProxy(endpoint)

def getApiProxyRecentGames(playerTag):

    endpoint = "players/%23" + playerTag + "/battlelog"

    return requestApiProxy(endpoint).get("items", [])

def getApiRecentGames(playerTag):
    response = requests.get(
            f"https://api.brawlstars.com/v1/players/%23{playerTag}/battlelog",
            headers={"Authorization": f"Bearer {BRAWL_API_KEY}"}
        )
    if response.status_code == 200:
        return response.json().get("items", [])
    else:
        print(f"Failed to fetch data for {playerTag}. HTTP Status Code: {response.status_code}")
        print("Response Message:", response.text)
        return []