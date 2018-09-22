import requests
import pandas as pd
from params import headers, payload, payload2
import numpy as np
import time

class NBA(object):
    """docstring for NBA."""

    def __init__(self):

        self.url = 'https://stats.nba.com/stats/{0}'
        self.players_df = None

    #helpers
    def get_data(self, endpoint, payload, **kwargs):
        for key, item in kwargs.items():
            payload[key] = item
        r = requests.get(self.url.format(endpoint), headers = headers, params = payload)
        print (r.status_code)
        return r.json()

    def to_df(self, dict_json):
        cols = dict_json['resultSets'][0]['headers']
        data = [row for row in dict_json['resultSets'][0]['rowSet']]
        df = pd.DataFrame(data, columns = cols)
        return df

    def save_csv(self, df, path_str=''):
        df.to_csv(path_str, index=False)
        return True

    #second instance (split class here)
    def players(self):
        endpoint = 'leaguedashplayerbiostats'
        players_data = self.get_data(endpoint=endpoint, payload = payload)
        players_df = self.to_df(players_data)
        self.players_df = players_df
        return players_df

    def shots(self, PlayerID='2544', Season='2017-18'):
        endpoint = 'shotchartdetail'
        shots_data = self.get_data(endpoint=endpoint, payload = payload2, PlayerID = PlayerID,Season=Season)
        shots_df = self.to_df(shots_data)
        return shots_df

    def save_shots(self, Season='2017-18'):
        if self.players_df is None:
            self.players()

        final= pd.DataFrame()
        for PlayerID in np.unique(self.players_df['PLAYER_ID']):
            time.sleep(2)
            shots = self.shots(PlayerID = PlayerID, Season=Season)
            final = pd.concat([final, shots], axis=0)

        self.save_csv(final, 'shots_{0}.csv'.format(Season))

if __name__ == '__main__':
    nba = NBA()
    #james_shots = nba.shots(PlayerID='2544')
    #print (james_shots.head())
    nba.save_shots('2011-12')
    #nba.merge_shots()
