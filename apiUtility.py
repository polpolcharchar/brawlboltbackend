import requests
from dotenv import load_dotenv
import os

load_dotenv()

API_PROXY_URL = os.getenv("BRAWL_STARS_API_PROXY_URL")
BRAWL_API_KEY = os.getenv("BRAWL_API_KEY")

def getApiProxyPlayerInfo(playerTag):
    body = {
        "endpoint": "players/%23" + playerTag,
        "params": {}
    }

    try:
        response = requests.post(API_PROXY_URL, json=body)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return None

def getApiProxyRecentGames(playerTag):
    body = {
        "endpoint": "players/%23" + playerTag + "/battlelog",
        "params": {}
    }

    try:
        response = requests.post(API_PROXY_URL, json=body)
        response.raise_for_status()
        return response.json().get("items", [])
    except requests.exceptions.RequestException as e:
        return None

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