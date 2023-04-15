import os

import pandas as pd

def generate_usfl_player_season_stats(season:int,save=False):
    games_df = pd.read_csv(f'player_stats/game_stats/{season}_player_game_stats.csv')
    games_df['G'] = 1
    #print(games_df.columns)
    season_df = pd.DataFrame(games_df.groupby(['season','team','team_nickname','player_id','player_name'],as_index=False)[
        ['G','COMP','ATT','PASS_YDS','PASS_TD','PASS_INT',
        'RUSH','RUSH_YDS','RUSH_TD',
        'REC_TARGETS','REC','REC_YDS','REC_TD',
        'FUMBLES','FUMBLES_LOST', 'FF', 'FR', 'TOTAL', 'SOLO', 'AST', 'TFL', 'SACKS','INT', 'PD', 'DEF_TD',
        'FGM', 'FGA', 'FG%', 'XPM', 'XPA','XP%',
        'PUNTS', 'GROSS_PUNT_YDS', 'GROSS_PUNT AVG', 'NET_PUNT_YDS','NET_PUNT_AVG', 'PUNT_TB', 'PUNTS_IN_20', 'PUNTS_BLK',
        'PR', 'PR_YDS', 'PR_AVG', 'PR_TD', 
        'KR', 'KR_YDS', 'KR_AVG','KR_TD']].sum())
    
    season_max_df = pd.DataFrame(games_df.groupby(['season','team','team_nickname','player_id','player_name'],as_index=False)[['RUSH_LONG', 'REC_LONG', 'PUNT_LONG', 'FG_LONG', 'PR_LONG', 'KR_LONG']].max())

    season_df = pd.merge(season_df,season_max_df,how='left',left_on=['season','team','team_nickname','player_id','player_name'],right_on=['season','team','team_nickname','player_id','player_name'])
    season_df.loc[season_df['ATT'] > 0,'COMP%'] = (season_df['COMP'] / season_df['ATT']) * 100
    season_df['COMP%'] = season_df['COMP%'].round(3)

    season_df.loc[season_df['ATT'] > 0,'PASS_TD%'] = season_df['PASS_TD'] / season_df['ATT']
    season_df['PASS_TD%'] = season_df['PASS_TD%'].round(3)

    season_df.loc[season_df['ATT'] > 0,'PASS_INT%'] = season_df['PASS_INT'] / season_df['ATT']
    season_df['PASS_INT%'] = season_df['PASS_INT%'].round(3)

    season_df.loc[season_df['ATT'] > 0,'PASS_YPA'] = season_df['PASS_YDS'] / season_df['ATT']
    season_df['PASS_YPA'] = season_df['PASS_YPA'].round(3)

    season_df.loc[season_df['ATT'] > 0,'PASS_AY/A'] = (season_df['PASS_YDS'] + (20 * season_df['PASS_TD']) - (45 * season_df['PASS_INT'])) / season_df['ATT']
    season_df['PASS_AY/A'] = season_df['PASS_AY/A'].round(3)

    season_df.loc[season_df['COMP'] > 0,'PASS_YPC'] = season_df['PASS_YDS'] / season_df['COMP']
    season_df['PASS_YPC'] = season_df['PASS_YPC'].round(3)

    season_df.loc[season_df['G'] > 0,'PASS_YDS/G'] = season_df['PASS_YDS'] / season_df['G']
    season_df['PASS_YDS/G'] = season_df['PASS_YDS/G'].round(3)

    season_df[['COMP','ATT','PASS_YDS','PASS_TD','PASS_INT']] = season_df[['COMP','PASS_YPA','PASS_TD','ATT','PASS_INT']].fillna(0)

    ## NFL QB Rating 
    season_df.loc[season_df['ATT']>0,'NFL_QBR_A'] = ((season_df['COMP'] / season_df['ATT']) - 0.3) * 5
    season_df.loc[season_df['ATT']>0,'NFL_QBR_B'] = ((season_df['PASS_YDS'] / season_df['ATT']) - 3) * 0.25
    season_df.loc[season_df['ATT']>0,'NFL_QBR_C'] = (season_df['PASS_TD'] / season_df['ATT']) * 20
    season_df.loc[season_df['ATT']>0,'NFL_QBR_D'] = 2.375 - (season_df['PASS_INT']/season_df['ATT']* 25)

    season_df.loc[season_df['NFL_QBR_A'] < 0, 'NFL_QBR_A'] = 0
    season_df.loc[season_df['NFL_QBR_A'] > 2.375, 'NFL_QBR_A'] = 2.375
    
    season_df.loc[season_df['NFL_QBR_B'] < 0, 'NFL_QBR_B'] = 0
    season_df.loc[season_df['NFL_QBR_B'] > 2.375, 'NFL_QBR_B'] = 2.375
    
    season_df.loc[season_df['NFL_QBR_C'] < 0, 'NFL_QBR_C'] = 0
    season_df.loc[season_df['NFL_QBR_C'] > 2.375, 'NFL_QBR_C'] = 2.375

    season_df.loc[season_df['NFL_QBR_D'] < 0, 'NFL_QBR_D'] = 0
    season_df.loc[season_df['NFL_QBR_D'] > 2.375, 'NFL_QBR_D'] = 2.375

    season_df['NFL_QBR'] = ((season_df['NFL_QBR_A'] + season_df['NFL_QBR_B'] + season_df['NFL_QBR_C'] + season_df['NFL_QBR_D']) / 6) * 100

    season_df = season_df.drop(columns=['NFL_QBR_A','NFL_QBR_B','NFL_QBR_C','NFL_QBR_D'])

    season_df.loc[season_df['ATT'] > 0,'CFB_QBR'] = ((8.4 * season_df['PASS_YDS']) + (330 * season_df['PASS_TD']) + (100 * season_df['COMP']) - (200 * season_df['PASS_INT'])) / season_df['ATT']
    
    season_df.loc[season_df['RUSH'] > 0,'RUSH_AVG'] = season_df['RUSH_YDS'] / season_df['RUSH']
    season_df['RUSH_AVG'] = season_df['RUSH_AVG'].round(3)

    season_df.loc[season_df['G'] > 0,'RUSH_ATT/G'] = season_df['RUSH'] / season_df['G']
    season_df['RUSH_ATT/G'] = season_df['RUSH_ATT/G'].round(3)

    season_df.loc[season_df['G'] > 0,'RUSH_YDS/G'] = season_df['RUSH_YDS'] / season_df['G']
    season_df['RUSH_YDS/G'] = season_df['RUSH_YDS/G'].round(3)

    season_df.loc[season_df['REC'] > 0,'REC_AVG'] = season_df['REC_YDS'] / season_df['REC']
    season_df['REC_AVG'] = season_df['REC_AVG'].round(3)

    season_df.loc[season_df['REC_TARGETS'] > 0,'CATCH%'] = (season_df['REC'] / season_df['REC_TARGETS']) * 100
    season_df['CATCH%'] = season_df['CATCH%'].round(3)

    season_df.loc[season_df['REC_TARGETS'] > 0,'REC_YDS/TARGET'] = (season_df['REC_YDS'] / season_df['REC_TARGETS']) * 100
    season_df['REC_YDS/TARGET'] = season_df['REC_YDS/TARGET'].round(3)

    season_df.loc[season_df['G'] > 0,'REC_YDS/G'] = season_df['REC_YDS'] / season_df['G']
    season_df['REC_YDS/G'] = season_df['REC_YDS/G'].round(3)

    season_df.loc[season_df['FGM'] > 0,'FG%'] = season_df['FGM'] / season_df['FGA']
    season_df['FG%'] = season_df['FG%'].round(3)

    season_df.loc[season_df['FGM'] > 0,'GROSS_PUNT_AVG'] = season_df['GROSS_PUNT_YDS'] / season_df['PUNTS']
    season_df['GROSS_PUNT_AVG'] = season_df['GROSS_PUNT_AVG'].round(3)

    season_df['NET_PUNT_YDS'] = None
    season_df['NET_PUNT_AVG'] = None
    
    season_df.loc[season_df['PR'] > 0,'PR_AVG'] = season_df['PR_YDS'] / season_df['PR']
    season_df['PR_AVG'] = season_df['PR_AVG'].round(3)

    season_df.loc[season_df['PR'] > 0,'PR_AVG'] = season_df['PR_YDS'] / season_df['PR']
    season_df['PR_AVG'] = season_df['PR_AVG'].round(3)

    season_df.to_csv('test.csv')
    #print(season_df.columns)
    cols = ['season', 'team', 'team_nickname', 'player_id', 'player_name', 'G',
       'COMP', 'ATT', 'COMP%', 'PASS_YDS', 'PASS_TD', 'PASS_TD%', 'PASS_INT', 'PASS_INT%', 'PASS_YPA', 'PASS_AY/A','PASS_YPC', 'PASS_YDS/G', 'NFL_QBR', 'CFB_QBR', 
       'RUSH', 'RUSH_YDS', 'RUSH_TD', 'RUSH_AVG', 'RUSH_LONG','RUSH_ATT/G', 'RUSH_YDS/G', 
       'REC_TARGETS', 'REC', 'REC_YDS','REC_AVG', 'REC_TD', 'CATCH%', 'REC_YDS/TARGET', 'REC_YDS/G',
       'FUMBLES','FUMBLES_LOST', 'FF', 'FR', 
       'TOTAL', 'SOLO', 'AST', 'TFL', 'SACKS',
       'INT', 'PD', 'DEF_TD', 
       'FGM', 'FGA', 'FG%', 'FG_LONG', 'XPM', 'XPA','XP%', 
       'PUNTS', 'GROSS_PUNT_YDS', 'GROSS_PUNT AVG', 'NET_PUNT_YDS', 'NET_PUNT_AVG', 'PUNT_TB', 'PUNTS_IN_20', 'PUNTS_BLK', 'PUNT_LONG',
       'PR', 'PR_YDS', 'PR_AVG', 'PR_TD', 'PR_LONG', 
       'KR', 'KR_YDS', 'KR_AVG', 'KR_TD', 'KR_LONG'
       ]
    
    season_df = season_df[cols]
    
    if save == True:
        season_df.to_csv(f'player_stats/season_stats/csv/{season}_player_stats.csv',index=False)
        season_df.to_parquet(f'player_stats/season_stats/parquet/{season}_player_stats.parquet',index=False)
    
    return season_df

def main():
    print("Starting up!")
    generate_usfl_player_season_stats(2023,True)
    print("All done!")

if __name__ == "__main__":
    main()