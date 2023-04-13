import pandas as pd
import os

from usfl import reformatFolderString

def generate_usfl_player_season_stats(season:int):
    games_df = pd.read_csv(f'player_stats/game_stats/{season}_player_game_stats.csv')
    games_df['G'] = 1
    season_df = pd.DataFrame(games_df.groupby(['season','team','team_nickname','player_id','player_name'],as_index=False)['G','COMP','ATT','COMP%','PASS_YDS','PASS_TD','PASS_INT','YPA','YPC','RUSH','RUSH_YDS','RUSH_AVG','RUSH_TD'].sum())
    print(season_df)
    
def main():
    print("Starting up!")
    generate_usfl_player_season_stats(2022)
    print("All done!")

if __name__ == "__main__":
    main()