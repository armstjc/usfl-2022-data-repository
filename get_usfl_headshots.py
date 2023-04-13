import time
import urllib.request

import pandas as pd
from tqdm import tqdm
import ssl


def get_usfl_headshots(season:int):
    """
    
    """

    ssl._create_default_https_context = ssl._create_unverified_context
    
    players_df = pd.read_csv(f'rosters/season/csv/{season}_usfl_rosters.csv')
    players_df = players_df.dropna(subset=['player_headshot'])
    player_ids_arr = players_df['player_id'].to_list()
    player_headshots_arr = players_df['player_headshot'].to_list()

    for i in tqdm(range(0,len(player_ids_arr))):
        url = player_headshots_arr[i]
        player_id = player_ids_arr[i]

        if url == 'https://b.fssta.com/uploads/application/fs-app/default-headshot.vresize.140.170.medium.0.png':
            pass ## This URL coresponds to the default (blank) headshot. We don't need to save that, so we skip players who's headshot url is equal to this URL.
        else:
            try:
                urllib.request.urlretrieve(url,filename=f'rosters/headshots/{player_id}.png')
            except:
                print(f'\nCould not retrive the photo for player #{player_id}.')
            time.sleep(1)

def main():
    print('Starting up...')
    get_usfl_headshots(2023)

if __name__ == "__main__":
    main()