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

def downloadUsflGame(gameID:int,apiKey:str):
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
        urllib.request.urlretrieve(url, filename=f"Gamelogs/{gameID}.json")
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
        game_date = data['header']['eventTime']
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
    #print(main_df.dtypes)
    #print(main_df)

    if saveResults == True:
        maxSeason = main_df['season'].max()
        minSeason = main_df['season'].min()

        for i in range(maxSeason,minSeason+1):
            main_df.to_csv(f'schedules/{i}_schedule.csv',index=False)
    else:
        pass

    return main_df

def parseUsflPlayerStats(game_jsons:list,saveResults=False):
    main_df = pd.DataFrame()
    game_df = pd.DataFrame()
    s_df = pd.DataFrame()
    
    passing_df = pd.DataFrame()
    rush_df = pd.DataFrame()
    receiving_df = pd.DataFrame()
    defensive_df = pd.DataFrame()
    fumbles_df = pd.DataFrame()
    kick_return_df = pd.DataFrame()
    punt_return_df = pd.DataFrame()
    kicking_df = pd.DataFrame()
    punting_df = pd.DataFrame()

    index_0 = ""
    #game_df = pd.DataFrame()
    
    for i in tqdm(game_jsons): 
        game_df = pd.DataFrame()
        with open(i, 'r',encoding='utf8') as j:
            data = json.load(j)
        game_id = data['header']['id']
        game_date = data['header']['eventTime']
        game_date = game_date[:10]
        season = game_date[:4]

        away_team_id = data['header']['leftTeam']['name']
        away_team_nickname = data['header']['leftTeam']['longName']

        home_team_id = data['header']['rightTeam']['name']
        home_team_nickname = data['header']['rightTeam']['longName']
     
        for j in data['boxscore']['boxscoreSections']:
            team_title = j['title']
            if j['title'] != 'MATCHUP':
                for k in j['boxscoreItems']:
                    column_list = []
                    for l in k['boxscoreTable']['headers']:
                        for m in l['columns']:
                            if m['index'] == 0:
                                index_0 = m['text']
                                #print(index_0)
                                column_list.append('player_name')
                            else:
                                column_list.append(m['text'])
                    #print(column_list)
                    for l in k['boxscoreTable']['rows']:
                        stat_column = []
                        
                        for m in l['columns']:
                            stat_column.append(m['text'])
                        
                        s_df = pd.DataFrame(columns=column_list,data=[stat_column])
                        s_df['season'] = season
                        s_df['game_id'] = game_id
                        s_df['game_date'] = game_date

                        if team_title == away_team_nickname:
                            s_df['team'] = away_team_id
                            s_df['team_nickname'] = team_title
                            s_df['loc'] = 'A'
                            s_df['opponent'] = home_team_id
                            s_df['opponent_nickname'] = home_team_nickname
                        elif team_title == home_team_nickname:
                            s_df['team'] = home_team_id
                            s_df['team_nickname'] = team_title
                            s_df['loc'] = 'H'
                            s_df['opponent'] = away_team_id
                            s_df['opponent_nickname'] = away_team_nickname
                        else:
                            pass

                        try:
                            s_df['analytics_id'] = l['entityLink']['analyticsName']
                        except:
                            pass
                        try:
                            s_df['player_name'] = str(l['entityLink']['title']).title()
                        except:
                            pass
                        try:
                            s_df['player_id'] = l['entityLink']['layout']['tokens']['id']
                        except:
                            pass
                        try:
                            s_df['player_image'] = l['entityLink']['imageUrl']
                        except:
                            pass
                    
                        if index_0 == "PASSING":
                            passing_df = pd.concat([passing_df,s_df],ignore_index=True)
                            
                        elif index_0 == "RUSHING":
                            rush_df = pd.concat([rush_df,s_df],ignore_index=True)
                        elif index_0 == "RECEIVING":
                            receiving_df = pd.concat([receiving_df,s_df],ignore_index=True)
                        elif index_0 == "DEFENSIVE":
                            defensive_df = pd.concat([defensive_df,s_df],ignore_index=True)
                        elif index_0 == "FUMBLES":
                            fumbles_df = pd.concat([fumbles_df,s_df],ignore_index=True)
                        elif index_0 == "KICK RETURN":
                            kick_return_df = pd.concat([kick_return_df,s_df],ignore_index=True)
                        elif index_0 == "PUNT RETURN":
                            punt_return_df = pd.concat([punt_return_df,s_df],ignore_index=True)
                        elif index_0 == "KICKING":
                            kicking_df = pd.concat([kicking_df,s_df],ignore_index=True)
                        elif index_0 == "PUNTING":
                            punting_df = pd.concat([punting_df,s_df],ignore_index=True)
                        else:
                            print(f'Need DF for {index_0}')
    pass_column_names = ['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name','COMP','ATT','COMP%','PASS YDS','PASS TD','PASS INT','NFL QBR','YPA','YPC']
    try:
        passing_df[['COMP','ATT']] = passing_df['COM'].str.split('/',expand=True)
        passing_df.drop(['COM'],axis=1,inplace=True)
    except:
        pass

    ## PASSING
    passing_df = passing_df.drop_duplicates()
    passing_df.rename(columns={'PCT':'COMP%','YDS':'PASS YDS','AVG':'YPA','TD':'PASS TD','INT':'PASS INT','QBR':'NFL QBR'},inplace=True)
    passing_df['NFL QBR'] = passing_df['NFL QBR'].replace(['-'],None)
    passing_df[['COMP','ATT','COMP%','PASS YDS','YPA','PASS TD','PASS INT','NFL QBR']] = passing_df[['COMP','ATT','COMP%','PASS YDS','YPA','PASS TD','PASS INT','NFL QBR']].apply(pd.to_numeric)
    passing_df['COMP%'] = (passing_df['COMP'] / passing_df['ATT']) *100
    passing_df['YPA'] = (passing_df['PASS YDS']/passing_df['COMP'])
    passing_df['YPC'] = (passing_df['PASS YDS']/passing_df['COMP'])
    passing_df = passing_df.reindex(columns=pass_column_names)
    #passing_df.to_csv('test_pass.csv',index=False)

    ## RUSHING
    rush_df = rush_df.drop_duplicates()
    rush_column_names = ['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name','RUSH','RUSH YDS','RUSH AVG','RUSH TD','RUSH LONG']
    rush_df.rename(columns={'ATT':'RUSH','YDS':'RUSH YDS','AVG':'RUSH AVG','TD':'RUSH TD','LNG':'RUSH LONG'},inplace=True)
    rush_df[['RUSH','RUSH YDS','RUSH AVG','RUSH TD','RUSH LONG']] = rush_df[['RUSH','RUSH YDS','RUSH AVG','RUSH TD','RUSH LONG']].apply(pd.to_numeric)
    rush_df['RUSH AVG'] = (rush_df['RUSH YDS']/rush_df['RUSH'])
    rush_df = rush_df.reindex(columns=rush_column_names)
    #rush_df.to_csv('test_rush.csv',index=False)

    ## Reciving
    receiving_df = receiving_df.drop_duplicates()
    rec_column_names = ['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name','REC TARGETS','REC','REC YDS','REC AVG','REC TD','REC LONG','CATCH%','YDS/TARGET']
    receiving_df.rename(columns={'TGT':'REC TARGETS','YDS':'REC YDS','AVG':'REC AVG','TD':'REC TD','LNG':'REC LONG'},inplace=True)
    receiving_df['REC AVG'] = receiving_df['REC AVG'].replace(['-'],None)
    receiving_df[['REC TARGETS','REC','REC YDS','REC AVG','REC TD','REC LONG']] = receiving_df[['REC TARGETS','REC','REC YDS','REC AVG','REC TD','REC LONG']].apply(pd.to_numeric)
    receiving_df['REC AVG'] = (receiving_df['REC YDS']/receiving_df['REC'])
    receiving_df['CATCH%'] = (receiving_df['REC']/receiving_df['REC TARGETS']) * 100
    receiving_df['YDS/TARGET'] = (receiving_df['REC YDS']/receiving_df['REC'])
    receiving_df = receiving_df.reindex(columns=rec_column_names)
    #receiving_df.to_csv('test_rec.csv',index=False)

    ## Fumbles
    fumbles_df = fumbles_df.drop_duplicates()
    fum_column_names = ['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name','FUMBLES','FUMBLES LOST','FF','FR']
    fumbles_df.rename(columns={'FUM':'FUMBLES','LST':'FUMBLES LOST','REC':'FR'},inplace=True)
    #fumbles_df[['FUMBLES','FUMBLES LOST','FF','FR']] = fumbles_df[['FUMBLES','FUMBLES LOST','FF','FR']].apply(pd.to_numeric)
    fumbles_df = fumbles_df.reindex(columns=fum_column_names)
    #fumbles_df.to_csv('test_fum.csv',index=False)

    ## Defense
    defensive_df = defensive_df.drop_duplicates()
    def_column_names = ['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name','TOTAL','SOLO','AST','TFL','SACKS','INT','PD','DEF TD']
    defensive_df.rename(columns={'TCK':'TOTAL','SOL':'SOLO','SCK':'SACKS','TD':'DEF TD'},inplace=True)
    defensive_df[['TOTAL','SOLO','TFL','SACKS','INT','PD','DEF TD']] = defensive_df[['TOTAL','SOLO','TFL','SACKS','INT','PD','DEF TD']].apply(pd.to_numeric)
    defensive_df['AST'] = defensive_df['TOTAL'] - defensive_df['SOLO']
    defensive_df = defensive_df.reindex(columns=def_column_names)
    #defensive_df.to_csv('test_def.csv',index=False)
    
    ## FG Kicking
    kicking_df = kicking_df.drop_duplicates()
    kicking_column_names = ['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name','FGM','FGA','FG%','FG LONG','XPM','XPA','XP%']
    kicking_df.drop(['PTS','PCT'],axis=1,inplace=True)
    kicking_df[['FGM','FGA']] = kicking_df['FG'].str.split('/',expand=True)
    kicking_df.drop(['FG'],axis=1,inplace=True)
    
    kicking_df[['XPM','XPA']] = kicking_df['XP'].str.split('/',expand=True)
    kicking_df.drop(['XP'],axis=1,inplace=True)
    kicking_df.rename(columns={'LNG':'FG LONG'},inplace=True)
    kicking_df['FG LONG'] = kicking_df['FG LONG'].replace(['-'],None)
    kicking_df[['FGM','FGA','XPM','XPA']] = kicking_df[['FGM','FGA','XPM','XPA']].apply(pd.to_numeric)
    kicking_df['FG%'] = kicking_df['FGM']/kicking_df['FGA']
    kicking_df['XP%'] = kicking_df['XPM']/kicking_df['XPA']
    kicking_df = kicking_df.reindex(columns=kicking_column_names)
    #kicking_df.to_csv('test_kick.csv',index=False)

    ## Punting
    punting_df = punting_df.drop_duplicates()
    punting_column_names = ['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name',
    'PUNTS','GROSS PUNT YDS','GROSS PUNT AVG','PUNT TB','PUNTS IN 20','PUNTS BLK','PUNT LONG']
    punting_df.rename(columns={'NO':'PUNTS','AVG':'GROSS PUNT AVG','20':'PUNTS IN 20','TB':'PUNT TB','LNG':'PUNT LONG','BLK':'PUNTS BLK'},inplace=True)
    punting_df[['PUNTS','GROSS PUNT AVG','PUNT TB','PUNTS IN 20','PUNTS BLK','PUNT LONG']] = punting_df[['PUNTS','GROSS PUNT AVG','PUNT TB','PUNTS IN 20','PUNTS BLK','PUNT LONG']].apply(pd.to_numeric)
    punting_df['GROSS PUNT YDS'] = (punting_df['GROSS PUNT AVG']*punting_df['PUNTS']).astype('int')
    punting_df = punting_df.reindex(columns=punting_column_names)
    #punting_df.to_csv('test_punt.csv',index=False)
    
    ## KR
    kick_return_df = kick_return_df.drop_duplicates()
    kr_column_names = ['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name','KR','KR YDS','KR AVG','KR TD','KR LONG']
    kick_return_df.rename(columns={'RET':'KR','YDS':'KR YDS','AVG':'KR AVG','LNG':'KR LONG','TD':'KR TD'},inplace=True)
    kick_return_df[['KR','KR YDS','KR AVG','KR TD','KR LONG']] = kick_return_df[['KR','KR YDS','KR AVG','KR TD','KR LONG']].apply(pd.to_numeric)
    kick_return_df['KR AVG'] = kick_return_df['KR YDS']/kick_return_df['KR']
    kick_return_df = kick_return_df.reindex(columns=kr_column_names)
    #kick_return_df.to_csv('test_kr.csv',index=False)
    
    ## PR
    punt_return_df = punt_return_df.drop_duplicates()
    pr_column_names = ['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name','PR','PR YDS','PR AVG','PR TD','PR LONG']
    punt_return_df.rename(columns={'RET':'PR','YDS':'PR YDS','AVG':'PR AVG','LNG':'PR LONG','TD':'PR TD'},inplace=True)
    punt_return_df['PR LONG'] = punt_return_df['PR LONG'].replace(['-'],None)
    punt_return_df['PR AVG'] = punt_return_df['PR AVG'].replace(['-'],None)
    punt_return_df[['PR','PR YDS','PR AVG','PR TD','PR LONG']] = punt_return_df[['PR','PR YDS','PR AVG','PR TD','PR LONG']].apply(pd.to_numeric)
    punt_return_df['PR AVG'] = (punt_return_df['PR YDS']/punt_return_df['PR'])
    punt_return_df = punt_return_df.reindex(columns=pr_column_names)
    #punt_return_df.to_csv('test_pr.csv',index=False)
    
    main_df = passing_df
    main_df = pd.merge(main_df,rush_df, on=['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name'],how='outer')
    main_df = pd.merge(main_df,receiving_df, on=['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name'],how='outer')
    main_df = pd.merge(main_df,fumbles_df, on=['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name'],how='outer')
    main_df = pd.merge(main_df,defensive_df, on=['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name'],how='outer')
    main_df = pd.merge(main_df,kicking_df, on=['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name'],how='outer')
    main_df = pd.merge(main_df,punting_df, on=['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name'],how='outer')
    main_df = pd.merge(main_df,punt_return_df, on=['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name'],how='outer')
    main_df = pd.merge(main_df,kick_return_df, on=['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name'],how='outer')
    main_df = main_df[main_df['player_name']!='TOTALS']
    main_df = main_df.drop_duplicates(subset=['season','game_id','game_date','team','team_nickname','loc','opponent','opponent_nickname','analytics_id','player_id','player_image','player_name'],keep='last')
    
    if saveResults == True:
        min_season = int(main_df['season'].min())
        max_season = int(main_df['season'].max())
        for i in range(min_season,max_season+1):
            main_df.to_csv(f'player_stats/{i}_player_stats.csv',index=False)

    return main_df

def parseUsflPbp(game_jsons:list,saveResults=False):
    main_df = pd.DataFrame()
    game_df = pd.DataFrame()
    play_df = pd.DataFrame()
    for i in tqdm(game_jsons): 
        away_score = 0
        home_score = 0
        
        game_df = pd.DataFrame()
        
        with open(i, 'r',encoding='utf8') as j:
            data = json.load(j)
        
        game_id = data['header']['id']
        game_date = data['header']['eventTime']
        game_date = game_date[:10]
        season = game_date[:4]

        away_team_id = data['header']['leftTeam']['name']
        away_team_nickname = data['header']['leftTeam']['longName']
        away_team_full_name = data['header']['leftTeam']['alternateName']

        home_team_id = data['header']['rightTeam']['name']
        home_team_nickname = data['header']['rightTeam']['longName']
        home_team_full_name = data['header']['rightTeam']['alternateName']

        for j in data['pbp']['sections']:
            quarter = j['title']
            print(quarter)

            for k in j['groups']:
                drive_play_num = 1
                drive_id = k['id']
                drive_result = k['title']
                drive_summary = str(k['subtitle'])
                drive_plays, drive_yards, drive_time = drive_summary.split(' · ')
                drive_plays = drive_plays.replace(' plays','')
                drive_yards = drive_yards.replace(' yards','')

                off_team_full_name = k['imageAltText']
                if off_team_full_name == away_team_full_name: ## Offense == away team
                    off_team_nickname = away_team_nickname
                    off_team_id = away_team_id
                    def_team_id = home_team_id
                    def_team_nickname = home_team_nickname
                    def_team_full_name = home_team_full_name

                elif off_team_full_name == home_team_full_name:
                    off_team_nickname = home_team_nickname
                    off_team_id = home_team_id
                    def_team_id = away_team_id
                    def_team_nickname = away_team_nickname
                    def_team_full_name = away_team_full_name
                for play in k['plays']:
                    #print(play['id'])
                    play_df = pd.DataFrame(columns=['game_id'],data=[game_id])
                    play_df['season'] = season
                    play_df['game_date'] = game_date
                    play_df['away_team_id'] = away_team_id
                    play_df['away_team_nickname'] = away_team_nickname
                    play_df['away_team_full_name'] = away_team_full_name
                    play_df['home_team_id'] = home_team_id
                    play_df['home_team_nickname'] = home_team_nickname
                    play_df['home_team_full_name'] = home_team_full_name
                    play_df['off_team_id'] = off_team_id
                    play_df['off_team_nickname'] = off_team_nickname
                    play_df['off_team_full_name'] = off_team_full_name
                    play_df['def_team_id'] = def_team_id
                    play_df['def_team_nickname'] = def_team_nickname
                    play_df['def_team_full_name'] = def_team_full_name
                    play_df['quarter'] = quarter
                    play_df['drive_id'] = drive_id
                    play_df['play_id'] = play['id']
                    
                    try:
                        down_and_distance = play['title']
                    except:
                        down_and_distance = None
                    play_df['down_and_distance'] = down_and_distance
                    play_down = 0
                    play_distance = 0
                    if down_and_distance == "END QUARTER" or down_and_distance == "KICKOFF" or down_and_distance == "PAT" or down_and_distance == None:
                        pass
                    else:
                        play_down, play_distance = down_and_distance.split(' AND ')
                        play_down = play_down[0]
                        #print(down)
                    play_df['down'] = play_down
                    play_df['distance'] = play_distance

                    try:
                        play_df['ball_on'] = play['subtitle']
                    except:
                        play_df['ball_on'] = None
                    
                    play_df['time_of_play'] = play['timeOfPlay']
                    play_df[['time_of_play_min','time_of_play_sec']] =play_df['time_of_play'].str.split(':',expand=True)

                    play_df['drive_play_num'] = drive_play_num
                    
                    play_df['play_description'] = play['playDescription']
                    play_df['away_team_score_change'] = play['leftTeamScoreChange']
                    play_df['home_team_score_change'] = play['rightTeamScoreChange']

                    try:
                        away_score = play['leftTeamScore']
                        home_score = play['rightTeamScore']
                    except:
                        pass
                    play_df['away_score'] = away_score
                    play_df['home_score'] = home_score

                    #drive_plays, drive_yards, drive_time
                    play_df['drive_plays'] = drive_plays
                    play_df['drive_yards'] = drive_yards
                    play_df['drive_time'] = drive_time
                    #kicking_df[['XPM','XPA']] = kicking_df['XP'].str.split('/',expand=True)
                    play_df[['drive_time_min','drive_time_sec']] =play_df['drive_time'].str.split(':',expand=True)
                    
                    play_df['drive_result'] = drive_result
                    game_df = pd.concat([game_df,play_df],ignore_index=True)
                    drive_play_num += 1
        main_df = pd.concat([main_df,game_df],ignore_index=True)
    main_df.to_csv('pbp/usfl_play_by_play.csv',index=False)
    print(main_df)




def main():
    print('Starting up')
    #key = "firefox" ## This is not a proper key. "firefox" is being used as a placeholder.
    key = os.environ['USFL_KEY']
    for i in tqdm(range(1,43)):
        downloadUsflGame(i,key)
    json_list = getJsonInFolder('Gamelogs')
    
    parseUsflPlayerStats(json_list,True)
    parseUsflPbp(json_list,True)

if __name__ == "__main__":
    main()
