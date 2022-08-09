"""
File: usfl.py
Author: Joseph Armstrong
Purpose: Download data related to the 2022 reboot of the USFL.
"""
import urllib.request
from tqdm import tqdm
import ssl
import time
import os
import json
import pandas as pd
ssl._create_default_https_context = ssl._create_unverified_context

def reformatFolderString(folder:str):
    """
    Reformats a string that represents a directory,
    into a string Python recognizes as a directory.

    Args:
        folder (str):
            Required parameter. Indicates the string you want
            to represent a directory, or a directory path.

    Returns:
        reform_folder (str):
            String intended to represent a directory, 
            or a directory path.
    """
    reform_folder = folder.replace("\\","/")
    print(f'Working Directory: \n\t{reform_folder}')
    return reform_folder

def getJsonInFolder(folder:str):
    """
    Retrives a list of JSON files in a given directory.
    Be warned, this may not work recursively.

    Args:
        folder (str):
            Required parameter. Indicates the string you want
            to represent a directory, or a directory path.
    
    Returns:
        json_list (list):
            A list of JSON files 
    """
    abs_path = os.path.abspath(folder)
    json_list = []
    dir_list = os.listdir(abs_path)
    #print(dir_list)
    file_list = list(filter(lambda x: '.json' in x, dir_list))
    #print(l)
    for i in file_list:
        json_list.append(abs_path+'/'+i)
    #arr = list(filter(lambda x: '.json' in x, f))
    #print(f)
    print(f'{len(json_list)} JSON files found in:\n\t {abs_path}')
    
    return json_list

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

    Example:
        dowloadUsflGame(1,2022,"api-key-placeholder")
    """
    url = f"https://api.foxsports.com/bifrost/v1/usfl/event/{gameID}/data?apikey={apiKey}"
    try:
        urllib.request.urlretrieve(url, filename=f"Gamelogs/{season}/{gameID}.json")
        time.sleep(5)
    except:
        time.sleep(5)

def parseUsflSchedule(game_jsons:list,saveResults=False):
    #print(game_jsons)
    main_df = pd.DataFrame()
    for i in tqdm(game_jsons): 
        with open(i, 'r',encoding='utf8') as j:
            data = json.load(j)
        #print(f'data: \n {data}')
        #print(data['header']['id'],data['header']['socialStartTime'])
        game_id = data['header']['id']
        game_date = data['header']['socialStartTime']
        game_df = pd.DataFrame(columns=['game_id'],data=[game_id])
        game_df['season'] = game_date[:4]
        game_df['analytics_description']  = data['header']['analyticsDescription']
        game_df['event_status'] = data['header']['eventStatus']
        game_df['is_tba'] = data['header']['isTba']
        game_df['game_start'] = data['header']['socialStartTime']
        game_df['game_end'] = data['header']['socialStopTime']
        game_df['status_line'] = data['header']['statusLine']
        game_df['venue_name'] = data['header']['venueName']
        game_df['venue_location'] = data['header']['venueLocation']
        game_df['share_text'] = data['header']['shareText']
        game_df['importance'] = data['header']['importance']

        ## data['header']['leftTeam'] == Away Team
        game_df['away_team_abv'] = data['header']['leftTeam']['name']
        game_df['away_team_nickname'] = data['header']['leftTeam']['longName']
        game_df['away_team_full_name'] = data['header']['leftTeam']['alternateName']
        game_df['away_team_record'] = data['header']['leftTeam']['record']
        game_df['away_team_score'] = data['header']['leftTeam']['score']
        game_df['away_team_is_loser'] = data['header']['leftTeam']['isLoser']
        game_df['away_team_has_possession'] = data['header']['leftTeam']['hasPossession']

        ## data['header']['rightTeam'] == Home Team
        game_df['home_team_abv'] = data['header']['rightTeam']['name']
        game_df['home_team_nickname'] = data['header']['rightTeam']['longName']
        game_df['home_team_full_name'] = data['header']['rightTeam']['alternateName']
        game_df['home_team_record'] = data['header']['rightTeam']['record']
        game_df['home_team_score'] = data['header']['rightTeam']['score']
        game_df['home_team_is_loser'] = data['header']['rightTeam']['isLoser']
        game_df['home_team_has_possession'] = data['header']['rightTeam']['hasPossession']

        try:
            game_df['additional_title'] = data['metadata']['parameters']['additionalTitle']
        except:
            pass

        try:
            game_df['event_title'] = data['metadata']['parameters']['eventTitle']
        except:
            pass
        main_df = pd.concat([game_df,main_df],ignore_index=True)
        
    
    main_df = main_df.astype({"game_id":int,"season":int})
    main_df = main_df.infer_objects()
    main_df = main_df.sort_values('game_id')
    print(main_df.dtypes)
    print(main_df)

    if saveResults == True:
        maxSeason = main_df['season'].max()
        minSeason = main_df['season'].min()

        for i in range(maxSeason,minSeason+1):
            main_df.to_csv(f'schedules/{i}_schedule.csv',index=False)
    else:
        pass

    return main_df

def main():
    print('Starting up')
    key = "firefox" ## This is not a proper key. "firefox" is being used as a placeholder.
    #downloadUsflGame(1,2022,key)
    json_list = getJsonInFolder('Gamelogs/2022')
    parseUsflSchedule(json_list,True)

    #print(os.path.abspath('Gamelogs/2022'))
if __name__ == "__main__":
    main()