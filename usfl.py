"""
File: usfl.py
Author: Joseph Armstrong
Purpose: Download data related to the 2022 reboot of the USFL.
"""
from urllib.request import urlopen
from tqdm import tqdm
import ssl
import time
import os
import json
import pandas as pd

from get_usfl_api_key import get_usfl_api_key
ssl._create_default_https_context = ssl._create_unverified_context


def reformatFolderString(folder: str):
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
    reform_folder = folder.replace("\\", "/")
    print(f'Working Directory: \n\t{reform_folder}')
    return reform_folder


def get_json_in_folder(folder: str):
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
    # print(dir_list)
    file_list = list(filter(lambda x: '.json' in x, dir_list))
    # print(l)
    for i in file_list:
        json_list.append(abs_path+'/'+i)
    # arr = list(filter(lambda x: '.json' in x, f))
    # print(f)
    print(f'{len(json_list)} JSON files found in:\n\t {abs_path}')

    return json_list


def get_usfl_game(gameID: int, apiKey: str, save=False):
    """
    Retrives game data for a USFL game, given a proper
    USFL game ID.

    Args:
        gameID (int):
            Required parameter. Indicates the gameID you are
            trying to get data from.

        apiKey (str):
            Required parameter. You must pass a proper API Key
            you have from the USFL. Otherwise, this function will
            not work.

    Example:
        dowloadUsflGame(1,"api-key-placeholder",False)
    """
    url = f"https://api.foxsports.com/bifrost/v1/usfl/event/{gameID}/data?apikey={apiKey}"
    # try:
    #     urllib.request.urlretrieve(url, filename=f"Gamelogs/{gameID}.json")
    #     time.sleep(5)
    # except:
    #     time.sleep(5)
    response = urlopen(url)
    json_data = json.loads(response.read())
    time.sleep(1)
    if save == True:
        with open(f"Gamelogs/{gameID}.json", "w+") as f:
            f.write(json.dumps(json_data, indent=2))

    return json_data


def get_usfl_rosters(season: int, apiKey: str, week=0, save=False):
    rosters_df = pd.DataFrame()
    row_df = pd.DataFrame()

    teams_df = pd.read_csv('teams/usfl_teams.csv')

    team_ids_arr = teams_df['team_id'].to_list()
    team_analytics_arr = teams_df['team_analytics_name'].to_list()
    team_name_arr = teams_df['team_name'].to_list()

    for i in tqdm(range(0, len(team_ids_arr))):

        team_id = team_ids_arr[i]
        team_analytics_name = team_analytics_arr[i]
        team_name = team_name_arr[i]

        print(f'\nGetting the {season} {team_name} roster.')
        url = f"https://api.foxsports.com/bifrost/v1/usfl/team/{team_id}/roster?apikey={apiKey}"

        response = urlopen(url)
        time.sleep(1)
        json_data = json.loads(response.read())

        for i in json_data['groups']:
            if len(i['rows']) > 1:
                for j in i['rows']:
                    # print(j)
                    row_df = pd.DataFrame(
                        {
                            'season': season,
                            'team_id': team_id,
                            'team_analytics_name': team_analytics_name,
                            'team_name_arr': team_name
                        },
                        index=[0]
                    )
                    row_df['jersey_number'] = str(
                        j['columns'][0]['superscript']).replace('#', '')

                    row_df['player_id'] = j['entityLink']['layout']['tokens']['id']
                    try:
                        row_df['player_analytics_name'] = j['entityLink']['analyticsName']
                    except:
                        row_df['player_analytics_name'] = None

                    row_df['player_name'] = j['columns'][0]['text']
                    row_df['player_pos'] = j['columns'][1]['text']
                    row_df['player_age'] = j['columns'][2]['text']
                    row_df['player_height'] = j['columns'][3]['text']
                    row_df['player_weight'] = str(
                        j['columns'][4]['text']).replace(' lbs', '')
                    row_df['player_college'] = j['columns'][5]['text']
                    row_df['player_headshot'] = j['entityLink']['imageUrl']
                    row_df['player_url'] = j['entityLink']['contentUri']

                    rosters_df = pd.concat(
                        [rosters_df, row_df], ignore_index=True)

    if save == True:
        rosters_df.to_csv(
            f'rosters/season/csv/{season}_usfl_rosters.csv', index=False)
        rosters_df.to_parquet(
            f'rosters/season/parquet/{season}_usfl_rosters.parquet', index=False)

        if week > 0:
            rosters_df.to_csv(
                f'rosters/weekly/csv/{season}_{week}_usfl_rosters.csv', index=False)
            rosters_df.to_parquet(
                f'rosters/weekly/parquet/{season}_{week}_usfl_rosters.parquet', index=False)
    return rosters_df


def get_usfl_schedule(game_jsons: list, save=False):
    main_df = pd.DataFrame()
    for i in tqdm(game_jsons):
        with open(i, 'r', encoding='utf8') as j:
            data = json.load(j)
        # print(f'data: \n {data}')
        # print(data['header']['id'],data['header']['socialStartTime'])
        game_id = data['header']['id']
        game_date = data['header']['eventTime']
        game_df = pd.DataFrame(columns=['game_id'], data=[game_id])
        game_df['season'] = game_date[:4]
        game_df['analytics_description'] = data['header']['analyticsDescription']
        game_df['event_status'] = data['header']['eventStatus']

        game_df['is_tba'] = data['header']['isTba']
        game_df['game_start'] = data['header']['socialStartTime']
        game_df['game_end'] = data['header']['socialStopTime']

        try:
            game_df['status_line'] = data['header']['statusLine']
        except:
            game_df['status_line'] = None

        game_df['venue_name'] = data['header']['venueName']
        game_df['venue_location'] = data['header']['venueLocation']
        game_df['share_text'] = data['header']['shareText']
        game_df['importance'] = data['header']['importance']

        # data['header']['leftTeam'] == Away Team
        game_df['away_team_abv'] = data['header']['leftTeam']['name']
        game_df['away_team_nickname'] = data['header']['leftTeam']['longName']
        game_df['away_team_full_name'] = data['header']['leftTeam']['alternateName']
        game_df['away_team_record'] = data['header']['leftTeam']['record']

        try:
            game_df['away_team_score'] = data['header']['leftTeam']['score']
        except:
            game_df['away_team_score'] = None

        game_df['away_team_is_loser'] = data['header']['leftTeam']['isLoser']
        game_df['away_team_has_possession'] = data['header']['leftTeam']['hasPossession']

        # data['header']['rightTeam'] == Home Team
        game_df['home_team_abv'] = data['header']['rightTeam']['name']
        game_df['home_team_nickname'] = data['header']['rightTeam']['longName']
        game_df['home_team_full_name'] = data['header']['rightTeam']['alternateName']
        game_df['home_team_record'] = data['header']['rightTeam']['record']
        try:
            game_df['home_team_score'] = data['header']['rightTeam']['score']
        except:
            game_df['home_team_score'] = None

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
        main_df = pd.concat([game_df, main_df], ignore_index=True)

    main_df = main_df.astype({"game_id": int, "season": int})
    main_df = main_df.infer_objects()
    main_df = main_df.sort_values('game_id')
    # print(main_df.dtypes)
    # print(main_df)

    if save == True:
        maxSeason = main_df['season'].max()
        minSeason = main_df['season'].min()

        for i in range(maxSeason, minSeason+1):
            main_df.to_csv(f'schedules/{i}_schedule.csv', index=False)
    else:
        pass

    return main_df


def get_usfl_standings(season: int, apiKey: str, save=False):

    main_df = pd.DataFrame()
    row_df = pd.DataFrame()
    url = f"https://api.foxsports.com/bifrost/v1/usfl/league/standings?season={season}&apikey={apiKey}"
    response = urlopen(url)
    json_data = json.loads(response.read())

    for i in json_data['standingsSections'][0]['standings']:
        # print(i['template'])
        division = i['headers'][0]['columns'][0]['text']
        for j in i['rows']:
            team_analytics_name = j['entityLink']['analyticsName']
            team_rank = j['columns'][0]['text']
            row_df = pd.DataFrame({'season': season, 'division': division, 'team_rank': team_rank,
                                  'team_analytics_name': team_analytics_name}, index=[0])
            row_df['league'] = j['entityLink']['analyticsSport']
            row_df['team_nickname'] = j['columns'][1]['text']
            row_df['team_name'] = j['columns'][1]['imageAltText']
            row_df['team_logo_url'] = j['columns'][1]['imageUrl']
            row_df['team_logo_alt_url'] = j['columns'][1]['alternateImageUrl']
            row_df['team_id'] = j['entityLink']['layout']['tokens']['id']
            row_df['overall_record_txt'] = j['columns'][2]['text']
            row_df[['overall_W', 'overall_L']
                   ] = row_df['overall_record_txt'].str.split('-', expand=True)
            # USFL should not have any ties, per their rulebook.
            row_df['overall_T'] = 0
            # However, if they do, this and other columns representing the number of ties should be updated to hold the actual number of ties this team has.
            row_df['overall_win_pct'] = j['columns'][3]['text']

            try:
                row_df['overall_points_scored'] = int(j['columns'][4]['text'])
            except:
                row_df['overall_points_scored'] = 0

            try:
                row_df['overall_points_allowed'] = int(j['columns'][5]['text'])
            except:
                row_df['overall_points_allowed'] = 0

            row_df['overall_point_diff'] = row_df['overall_points_scored'] - \
                row_df['overall_points_allowed']

            row_df['home_record_txt'] = j['columns'][6]['text']
            row_df[['home_W', 'home_L']] = row_df['home_record_txt'].str.split(
                '-', expand=True)
            row_df['home_T'] = 0

            try:
                row_df[['home_W', 'home_L']] = row_df[[
                    'home_W', 'home_L']].astype('int')
            except:
                print(
                    f'No games played for the {team_analytics_name} in this season so far.')
            try:
                row_df['home_win_pct'] = (row_df['home_W'] + row_df['home_T']) / (
                    row_df['home_W'] + row_df['home_L'] + row_df['home_T'])
                row_df['home_win_pct'] = row_df['home_win_pct'].round(3)
            except:
                row_df['home_win_pct'] = 0

            row_df['away_record_txt'] = j['columns'][7]['text']
            row_df[['away_W', 'away_L']] = row_df['away_record_txt'].str.split(
                '-', expand=True)
            row_df['away_T'] = 0

            try:
                row_df[['away_W', 'away_L']] = row_df[[
                    'away_W', 'away_L']].astype('int')
                row_df['away_win_pct'] = (row_df['away_W'] + row_df['away_T']) / (
                    row_df['away_W'] + row_df['away_L'] + row_df['away_T'])
                row_df['away_win_pct'] = row_df['away_win_pct'].round(3)
            except:
                row_df['away_win_pct'] = 0

            row_df['division_record_txt'] = j['columns'][8]['text']
            row_df[['division_W', 'division_L']
                   ] = row_df['division_record_txt'].str.split('-', expand=True)
            row_df['division_T'] = 0

            try:
                row_df[['division_W', 'division_L']] = row_df[[
                    'division_W', 'division_L']].astype('int')
                row_df['division_win_pct'] = (row_df['division_W'] + row_df['division_T']) / (
                    row_df['division_W'] + row_df['division_L'] + row_df['division_T'])
                row_df['division_win_pct'] = row_df['division_win_pct'].round(
                    3)
            except:
                row_df['division_win_pct'] = 0

            row_df['streak'] = j['columns'][9]['text']

            main_df = pd.concat([main_df, row_df], ignore_index=True)

    if save == True:
        # raise NotImplementedError('help')
        main_df.to_csv(
            f'standings/csv/{season}_usfl_standings.csv', index=False)
        # main_df.to_parquet(f'standings/parquet/{season}_usfl_standings.parquet',index=False)
        with open(f"standings/json/{season}_usfl_standings.json", "w+") as f:
            f.write(json.dumps(json_data, indent=2))

    return main_df


def parse_usfl_player_stats(game_jsons: list, saveResults=False):
    main_df = pd.DataFrame()
    # game_df = pd.DataFrame()
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
    # game_df = pd.DataFrame()

    for i in tqdm(game_jsons):
        # game_df = pd.DataFrame()
        with open(i, 'r', encoding='utf8') as j:
            data = json.load(j)
        game_id = data['header']['id']
        game_date = data['header']['eventTime']
        game_date = game_date[:10]
        season = game_date[:4]

        away_team_id = data['header']['leftTeam']['name']
        away_team_nickname = data['header']['leftTeam']['longName']

        home_team_id = data['header']['rightTeam']['name']
        home_team_nickname = data['header']['rightTeam']['longName']
        try:
            for j in data['boxscore']['boxscoreSections']:
                team_title = j['title']
                if j['title'] != 'MATCHUP':
                    for k in j['boxscoreItems']:
                        column_list = []
                        for l in k['boxscoreTable']['headers']:
                            for m in l['columns']:
                                if m['index'] == 0:
                                    index_0 = m['text']
                                    # print(index_0)
                                    column_list.append('player_name')
                                else:
                                    column_list.append(m['text'])
                        # print(column_list)
                        for l in k['boxscoreTable']['rows']:
                            stat_column = []

                            for m in l['columns']:
                                stat_column.append(m['text'])

                            s_df = pd.DataFrame(
                                columns=column_list, data=[stat_column])
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
                                s_df['player_name'] = str(
                                    l['entityLink']['title']).title()
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
                                passing_df = pd.concat(
                                    [passing_df, s_df], ignore_index=True)

                            elif index_0 == "RUSHING":
                                rush_df = pd.concat(
                                    [rush_df, s_df], ignore_index=True)
                            elif index_0 == "RECEIVING":
                                receiving_df = pd.concat(
                                    [receiving_df, s_df], ignore_index=True)
                            elif index_0 == "DEFENSIVE":
                                defensive_df = pd.concat(
                                    [defensive_df, s_df], ignore_index=True)
                            elif index_0 == "FUMBLES":
                                fumbles_df = pd.concat(
                                    [fumbles_df, s_df], ignore_index=True)
                            elif index_0 == "KICK RETURN":
                                kick_return_df = pd.concat(
                                    [kick_return_df, s_df], ignore_index=True)
                            elif index_0 == "PUNT RETURN":
                                punt_return_df = pd.concat(
                                    [punt_return_df, s_df], ignore_index=True)
                            elif index_0 == "KICKING":
                                kicking_df = pd.concat(
                                    [kicking_df, s_df], ignore_index=True)
                            elif index_0 == "PUNTING":
                                punting_df = pd.concat(
                                    [punting_df, s_df], ignore_index=True)
                            else:
                                print(f'Need DF for {index_0}')
        except:
            print(f'Cannot parse player game stats from {i}.')

    pass_column_names = ['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc', 'opponent', 'opponent_nickname', 'analytics_id',
                         'player_id', 'player_image', 'player_name', 'COMP', 'ATT', 'COMP%', 'PASS_YDS', 'PASS_TD', 'PASS_INT', 'NFL_QBR', 'YPA', 'YPC']
    try:
        passing_df[['COMP', 'ATT']] = passing_df['COM'].str.split(
            '/', expand=True)
        passing_df.drop(['COM'], axis=1, inplace=True)
    except:
        pass

    # PASSING
    passing_df = passing_df.drop_duplicates()
    passing_df.rename(columns={'PCT': 'COMP%', 'YDS': 'PASS_YDS', 'AVG': 'YPA',
                      'TD': 'PASS_TD', 'INT': 'PASS_INT', 'QBR': 'NFL_QBR'}, inplace=True)
    passing_df['NFL_QBR'] = passing_df['NFL_QBR'].replace(['-'], None)
    passing_df[['COMP', 'ATT', 'COMP%', 'PASS_YDS', 'YPA', 'PASS_TD', 'PASS_INT', 'NFL_QBR']] = passing_df[[
        'COMP', 'ATT', 'COMP%', 'PASS_YDS', 'YPA', 'PASS_TD', 'PASS_INT', 'NFL_QBR']].replace('-', '0')
    passing_df[['COMP', 'ATT', 'COMP%', 'PASS_YDS', 'YPA', 'PASS_TD', 'PASS_INT', 'NFL_QBR']] = passing_df[[
        'COMP', 'ATT', 'COMP%', 'PASS_YDS', 'YPA', 'PASS_TD', 'PASS_INT', 'NFL_QBR']].apply(pd.to_numeric)
    passing_df['COMP%'] = (passing_df['COMP'] / passing_df['ATT']) * 100
    passing_df['YPA'] = (passing_df['PASS_YDS']/passing_df['ATT'])
    passing_df['YPC'] = (passing_df['PASS_YDS']/passing_df['COMP'])
    passing_df = passing_df.reindex(columns=pass_column_names)
    # passing_df.to_csv('test_pass.csv',index=False)

    # RUSHING
    rush_df = rush_df.drop_duplicates()
    rush_column_names = ['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc', 'opponent', 'opponent_nickname',
                         'analytics_id', 'player_id', 'player_image', 'player_name', 'RUSH', 'RUSH_YDS', 'RUSH_AVG', 'RUSH_TD', 'RUSH_LONG']
    rush_df.rename(columns={'ATT': 'RUSH', 'YDS': 'RUSH_YDS',
                   'AVG': 'RUSH_AVG', 'TD': 'RUSH_TD', 'LNG': 'RUSH_LONG'}, inplace=True)
    rush_df[['RUSH', 'RUSH_YDS', 'RUSH_AVG', 'RUSH_TD', 'RUSH_LONG']] = rush_df[[
        'RUSH', 'RUSH_YDS', 'RUSH_AVG', 'RUSH_TD', 'RUSH_LONG']].replace('-', '0')
    rush_df[['RUSH', 'RUSH_YDS', 'RUSH_AVG', 'RUSH_TD', 'RUSH_LONG']] = rush_df[[
        'RUSH', 'RUSH_YDS', 'RUSH_AVG', 'RUSH_TD', 'RUSH_LONG']].apply(pd.to_numeric)
    rush_df['RUSH_AVG'] = (rush_df['RUSH_YDS']/rush_df['RUSH'])
    rush_df = rush_df.reindex(columns=rush_column_names)
    # rush_df.to_csv('test_rush.csv',index=False)

    # Reciving
    receiving_df = receiving_df.drop_duplicates()
    rec_column_names = ['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc', 'opponent', 'opponent_nickname', 'analytics_id',
                        'player_id', 'player_image', 'player_name', 'REC_TARGETS', 'REC', 'REC_YDS', 'REC_AVG', 'REC_TD', 'REC_LONG', 'CATCH%', 'YDS/TARGET']
    receiving_df.rename(columns={'TGT': 'REC_TARGETS', 'YDS': 'REC_YDS',
                        'AVG': 'REC_AVG', 'TD': 'REC_TD', 'LNG': 'REC_LONG'}, inplace=True)
    receiving_df['REC_AVG'] = receiving_df['REC_AVG'].replace(['-'], None)
    receiving_df[['REC_TARGETS', 'REC', 'REC_YDS', 'REC_AVG', 'REC_TD', 'REC_LONG']] = receiving_df[[
        'REC_TARGETS', 'REC', 'REC_YDS', 'REC_AVG', 'REC_TD', 'REC_LONG']].replace('-', '0')
    receiving_df[['REC_TARGETS', 'REC', 'REC_YDS', 'REC_AVG', 'REC_TD', 'REC_LONG']] = receiving_df[[
        'REC_TARGETS', 'REC', 'REC_YDS', 'REC_AVG', 'REC_TD', 'REC_LONG']].apply(pd.to_numeric)
    receiving_df['REC_AVG'] = (receiving_df['REC_YDS']/receiving_df['REC'])
    receiving_df['CATCH%'] = (
        receiving_df['REC']/receiving_df['REC_TARGETS']) * 100
    receiving_df['YDS/TARGET'] = (receiving_df['REC_YDS']/receiving_df['REC'])
    receiving_df = receiving_df.reindex(columns=rec_column_names)
    # receiving_df.to_csv('test_rec.csv',index=False)

    # Fumbles
    fumbles_df = fumbles_df.drop_duplicates()
    fum_column_names = ['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc', 'opponent', 'opponent_nickname',
                        'analytics_id', 'player_id', 'player_image', 'player_name', 'FUMBLES', 'FUMBLES_LOST', 'FF', 'FR']
    fumbles_df.rename(
        columns={'FUM': 'FUMBLES', 'LST': 'FUMBLES_LOST', 'REC': 'FR'}, inplace=True)
    fumbles_df = fumbles_df.reindex(columns=fum_column_names)
    fumbles_df[['FUMBLES', 'FUMBLES_LOST', 'FF', 'FR']] = fumbles_df[[
        'FUMBLES', 'FUMBLES_LOST', 'FF', 'FR']].replace('-', '0')
    fumbles_df[['FUMBLES', 'FUMBLES_LOST', 'FF', 'FR']] = fumbles_df[[
        'FUMBLES', 'FUMBLES_LOST', 'FF', 'FR']].apply(pd.to_numeric)
    # fumbles_df = fumbles_df.reindex(columns=fum_column_names)
    # fumbles_df.to_csv('test_fum.csv',index=False)

    # Defense
    defensive_df = defensive_df.drop_duplicates()
    def_column_names = ['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc', 'opponent', 'opponent_nickname',
                        'analytics_id', 'player_id', 'player_image', 'player_name', 'TOTAL', 'SOLO', 'AST', 'TFL', 'SACKS', 'INT', 'PD', 'DEF_TD']
    defensive_df.rename(columns={
                        'TCK': 'TOTAL', 'SOL': 'SOLO', 'SCK': 'SACKS', 'TD': 'DEF_TD'}, inplace=True)
    defensive_df[['TOTAL', 'SOLO', 'TFL', 'SACKS', 'INT', 'PD', 'DEF_TD']] = defensive_df[[
        'TOTAL', 'SOLO', 'TFL', 'SACKS', 'INT', 'PD', 'DEF_TD']].replace('-', '0')

    defensive_df[['TOTAL', 'SOLO', 'TFL', 'SACKS', 'INT', 'PD', 'DEF_TD']] = defensive_df[[
        'TOTAL', 'SOLO', 'TFL', 'SACKS', 'INT', 'PD', 'DEF_TD']].apply(pd.to_numeric)
    defensive_df['AST'] = defensive_df['TOTAL'] - defensive_df['SOLO']
    defensive_df = defensive_df.reindex(columns=def_column_names)
    # defensive_df.to_csv('test_def.csv',index=False)

    # FG Kicking
    kicking_df = kicking_df.drop_duplicates()
    kicking_column_names = ['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc', 'opponent', 'opponent_nickname',
                            'analytics_id', 'player_id', 'player_image', 'player_name', 'FGM', 'FGA', 'FG%', 'FG_LONG', 'XPM', 'XPA', 'XP%']
    # kicking_df = kicking_df.reindex(columns=kicking_column_names)

    kicking_df.drop(['PTS', 'PCT'], axis=1, inplace=True)
    kicking_df[['FGM', 'FGA']] = kicking_df['FG'].str.split('/', expand=True)
    kicking_df.drop(['FG'], axis=1, inplace=True)

    kicking_df[['XPM', 'XPA']] = kicking_df['XP'].str.split('/', expand=True)
    kicking_df.drop(['XP'], axis=1, inplace=True)
    kicking_df.rename(columns={'LNG': 'FG_LONG'}, inplace=True)
    kicking_df['FG_LONG'] = kicking_df['FG_LONG'].replace(['-'], None)

    kicking_df[['FGM', 'FGA', 'FG_LONG', 'XPM', 'XPA']] = kicking_df[[
        'FGM', 'FGA', 'FG_LONG', 'XPM', 'XPA']].replace('-', '0')

    kicking_df[['FGM', 'FGA', 'XPM', 'XPA']] = kicking_df[[
        'FGM', 'FGA', 'XPM', 'XPA']].apply(pd.to_numeric)
    kicking_df['FG%'] = kicking_df['FGM']/kicking_df['FGA']
    kicking_df['XP%'] = kicking_df['XPM']/kicking_df['XPA']
    # kicking_df.to_csv('test_kick.csv',index=False)

    # Punting
    punting_df = punting_df.drop_duplicates()
    punting_column_names = ['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc', 'opponent', 'opponent_nickname', 'analytics_id', 'player_id', 'player_image', 'player_name',
                            'PUNTS', 'GROSS_PUNT_YDS', 'GROSS_PUNT AVG', 'NET_PUNT_YDS', 'NET_PUNT_AVG', 'PUNT_TB', 'PUNTS_IN_20', 'PUNTS_BLK', 'PUNT_LONG']
    punting_df.rename(columns={'NO': 'PUNTS', 'AVG': 'GROSS_PUNT_AVG', '20': 'PUNTS_IN_20',
                      'TB': 'PUNT_TB', 'LNG': 'PUNT_LONG', 'BLK': 'PUNTS_BLK'}, inplace=True)
    punting_df[['PUNTS', 'GROSS_PUNT_AVG', 'PUNT_TB', 'PUNTS_IN_20', 'PUNTS_BLK', 'PUNT_LONG']] = punting_df[[
        'PUNTS', 'GROSS_PUNT_AVG', 'PUNT_TB', 'PUNTS_IN_20', 'PUNTS_BLK', 'PUNT_LONG']].replace('-', '0')
    punting_df[['PUNTS', 'GROSS_PUNT_AVG', 'PUNT_TB', 'PUNTS_IN_20', 'PUNTS_BLK', 'PUNT_LONG']] = punting_df[[
        'PUNTS', 'GROSS_PUNT_AVG', 'PUNT_TB', 'PUNTS_IN_20', 'PUNTS_BLK', 'PUNT_LONG']].apply(pd.to_numeric)
    punting_df['GROSS_PUNT_YDS'] = (
        punting_df['GROSS_PUNT_AVG']*punting_df['PUNTS']).astype('int')
    punting_df = punting_df.reindex(columns=punting_column_names)
    # punting_df.to_csv('test_punt.csv',index=False)

    # KR
    kick_return_df = kick_return_df.drop_duplicates()
    kr_column_names = ['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc', 'opponent', 'opponent_nickname',
                       'analytics_id', 'player_id', 'player_image', 'player_name', 'KR', 'KR_YDS', 'KR_AVG', 'KR_TD', 'KR_LONG']
    kick_return_df.rename(columns={'RET': 'KR', 'YDS': 'KR_YDS',
                          'AVG': 'KR_AVG', 'LNG': 'KR_LONG', 'TD': 'KR_TD'}, inplace=True)
    kick_return_df[['KR', 'KR_YDS', 'KR_AVG', 'KR_TD', 'KR_LONG']] = kick_return_df[[
        'KR', 'KR_YDS', 'KR_AVG', 'KR_TD', 'KR_LONG']].replace('-', '0')

    kick_return_df[['KR', 'KR_YDS', 'KR_AVG', 'KR_TD', 'KR_LONG']] = kick_return_df[[
        'KR', 'KR_YDS', 'KR_AVG', 'KR_TD', 'KR_LONG']].apply(pd.to_numeric)
    kick_return_df['KR_AVG'] = kick_return_df['KR_YDS']/kick_return_df['KR']
    kick_return_df = kick_return_df.reindex(columns=kr_column_names)
    # kick_return_df.to_csv('test_kr.csv',index=False)

    # PR
    punt_return_df = punt_return_df.drop_duplicates()
    pr_column_names = ['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc', 'opponent', 'opponent_nickname',
                       'analytics_id', 'player_id', 'player_image', 'player_name', 'PR', 'PR_YDS', 'PR_AVG', 'PR_TD', 'PR_LONG']
    punt_return_df.rename(columns={'RET': 'PR', 'YDS': 'PR_YDS',
                          'AVG': 'PR_AVG', 'LNG': 'PR_LONG', 'TD': 'PR_TD'}, inplace=True)
    punt_return_df['PR_LONG'] = punt_return_df['PR_LONG'].replace(['-'], None)
    punt_return_df['PR_AVG'] = punt_return_df['PR_AVG'].replace(['-'], None)
    punt_return_df[['PR', 'PR_YDS', 'PR_AVG', 'PR_TD', 'PR_LONG']] = punt_return_df[[
        'PR', 'PR_YDS', 'PR_AVG', 'PR_TD', 'PR_LONG']].replace('-', '0')

    punt_return_df[['PR', 'PR_YDS', 'PR_AVG', 'PR_TD', 'PR_LONG']] = punt_return_df[[
        'PR', 'PR_YDS', 'PR_AVG', 'PR_TD', 'PR_LONG']].apply(pd.to_numeric)
    punt_return_df['PR_AVG'] = (punt_return_df['PR_YDS']/punt_return_df['PR'])
    punt_return_df = punt_return_df.reindex(columns=pr_column_names)
    # punt_return_df.to_csv('test_pr.csv',index=False)

    main_df = passing_df
    main_df = pd.merge(main_df, rush_df, on=['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc',
                       'opponent', 'opponent_nickname', 'analytics_id', 'player_id', 'player_image', 'player_name'], how='outer')
    main_df = pd.merge(main_df, receiving_df, on=['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc',
                       'opponent', 'opponent_nickname', 'analytics_id', 'player_id', 'player_image', 'player_name'], how='outer')
    main_df = pd.merge(main_df, fumbles_df, on=['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc',
                       'opponent', 'opponent_nickname', 'analytics_id', 'player_id', 'player_image', 'player_name'], how='outer')
    main_df = pd.merge(main_df, defensive_df, on=['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc',
                       'opponent', 'opponent_nickname', 'analytics_id', 'player_id', 'player_image', 'player_name'], how='outer')
    main_df = pd.merge(main_df, kicking_df, on=['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc',
                       'opponent', 'opponent_nickname', 'analytics_id', 'player_id', 'player_image', 'player_name'], how='outer')
    main_df = pd.merge(main_df, punting_df, on=['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc',
                       'opponent', 'opponent_nickname', 'analytics_id', 'player_id', 'player_image', 'player_name'], how='outer')
    main_df = pd.merge(main_df, punt_return_df, on=['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc',
                       'opponent', 'opponent_nickname', 'analytics_id', 'player_id', 'player_image', 'player_name'], how='outer')
    main_df = pd.merge(main_df, kick_return_df, on=['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc',
                       'opponent', 'opponent_nickname', 'analytics_id', 'player_id', 'player_image', 'player_name'], how='outer')
    main_df = main_df[main_df['player_name'] != 'TOTALS']
    main_df = main_df.drop_duplicates(subset=['season', 'game_id', 'game_date', 'team', 'team_nickname', 'loc',
                                      'opponent', 'opponent_nickname', 'analytics_id', 'player_id', 'player_image', 'player_name'], keep='last')

    main_df = main_df.sort_values(
        by=['season', 'game_date', 'game_id', 'loc', 'player_id'])

    if saveResults == True:
        min_season = int(main_df['season'].min())
        max_season = int(main_df['season'].max())
        for i in range(min_season, max_season+1):
            main_df.to_csv(
                f'player_stats/game_stats/{i}_player_game_stats.csv', index=False)

    return main_df


def parse_usfl_pbp(game_jsons: list, saveResults=False):
    main_df = pd.DataFrame()
    game_df = pd.DataFrame()
    play_df = pd.DataFrame()
    for i in tqdm(game_jsons):
        away_score = 0
        home_score = 0

        game_df = pd.DataFrame()

        with open(i, 'r', encoding='utf8') as j:
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

        try:
            for j in data['pbp']['sections']:
                quarter = j['title']
                # print(f'\n{quarter}')

                for k in j['groups']:
                    drive_play_num = 1
                    drive_id = k['id']
                    drive_result = k['title']
                    drive_summary = str(k['subtitle'])
                    drive_plays, drive_yards, drive_time = drive_summary.split(
                        ' · ')
                    drive_plays = drive_plays.replace(' plays', '')
                    drive_yards = drive_yards.replace(' yards', '')

                    off_team_full_name = k['imageAltText']
                    if off_team_full_name == away_team_full_name:  # Offense == away team
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
                        # print(play['id'])
                        play_df = pd.DataFrame(
                            columns=['game_id'], data=[game_id])
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
                            play_down, play_distance = down_and_distance.split(
                                ' AND ')
                            play_down = play_down[0]
                            # print(down)
                        play_df['down'] = play_down
                        play_df['distance'] = play_distance

                        try:
                            play_df['ball_on'] = play['subtitle']
                        except:
                            play_df['ball_on'] = None

                        play_df['time_of_play'] = play['timeOfPlay']
                        play_df[['time_of_play_min', 'time_of_play_sec']
                                ] = play_df['time_of_play'].str.split(':', expand=True)

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

                        # drive_plays, drive_yards, drive_time
                        play_df['drive_plays'] = drive_plays
                        play_df['drive_yards'] = drive_yards
                        play_df['drive_time'] = drive_time
                        # kicking_df[['XPM','XPA']] = kicking_df['XP'].str.split('/',expand=True)
                        play_df[['drive_time_min', 'drive_time_sec']
                                ] = play_df['drive_time'].str.split(':', expand=True)

                        play_df['drive_result'] = drive_result
                        game_df = pd.concat(
                            [game_df, play_df], ignore_index=True)
                        drive_play_num += 1
            main_df = pd.concat([main_df, game_df], ignore_index=True)
        except:
            print(f'Cannot parse play-by-play data from game #{game_id}.')

    main_df[['game_id', 'play_id']] = main_df[[
        'game_id', 'play_id']].astype('int')

    main_df = main_df.sort_values(by=['game_id', 'play_id'])

    if saveResults == True:
        min_season = int(main_df['season'].min())
        max_season = int(main_df['season'].max())
        for i in range(min_season, max_season+1):
            main_df.to_csv(f'pbp/{i}_play_by_play.csv', index=False)

    # main_df.to_csv(f'pbp/usfl_play_by_play.csv',index=False)
    print(main_df)


def main():
    print('Starting up')
    key = get_usfl_api_key()
    for i in tqdm(range(44, 84)):
        get_usfl_game(i, key, True)
    json_list = get_json_in_folder('Gamelogs')

    parse_usfl_player_stats(json_list, True)
    parse_usfl_pbp(json_list, True)
    get_usfl_schedule(json_list, True)

    get_usfl_standings(2023, key, True)
    get_usfl_rosters(2023, key, 7, True)


if __name__ == "__main__":
    main()
