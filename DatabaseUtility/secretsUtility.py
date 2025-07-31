import os
from dotenv import load_dotenv

def getSecret(secretName):
    value = os.environ.get(secretName)
    if value is not None:
        return value

    load_dotenv()
    value = os.environ.get(secretName)
    if value is not None:
        return value
    
    raise KeyError(f"Secret '{secretName}' not found in environment or .env file")