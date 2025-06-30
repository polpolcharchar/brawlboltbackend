import requests
from dotenv import load_dotenv
import os

load_dotenv()
API_PROXY_URL = os.getenv("BRAWL_STARS_API_PROXY_URL")

def getApiPlayerInfo(playerTag):
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

def getApiRecentGames(playerTag):
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
