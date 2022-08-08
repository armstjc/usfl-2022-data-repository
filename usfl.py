"""
File: usfl.py
Author: Joseph Armstrong
Purpose: Download data related to the 2022 reboot of the USFL.
"""
import urllib.request
from tqdm import tqdm
import ssl
import time
ssl._create_default_https_context = ssl._create_unverified_context

def downloadUsflGame(gameID:int,season:int,apiKey:str):
    """
        Retrives game data for a USFL game, given a proper
        USFL game ID.

        Args:
        gameID (int):
            Required parameter. Indicates the gameID you are
            trying to get data from.
        season (int):
            Required parameter. Indicates which season you are 
            trying to get data from.
        apiKey (str):
            Required parameter. You must pass a proper API Key
            you have from the USFL. Otherwise, this function will
            not work.
    """
    url = f"https://api.foxsports.com/bifrost/v1/usfl/event/{gameID}/data?apikey={apiKey}"
    try:
        urllib.request.urlretrieve(url, filename=f"Gamelogs/{season}/{gameID}.json")
        time.sleep(5)
    except:
        time.sleep(5)

def main():
    print('Starting up')
    key = "firefox" ## This is not a proper key. "firefox" is being used as a placeholder
    #downloadUsflGame(1,2022,key)

if __name__ == "__main__":
    main()