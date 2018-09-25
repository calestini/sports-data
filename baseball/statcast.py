import requests
import time
import numpy as np
import pandas as pd
import datetime
import io

class Statcast(object):
	"""Documentation for Statcast
	"""
	def __init__(self):
		self.url = 'https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7CPO%7CS%7C=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={0}&game_date_lt={1}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details'
		self.payload = {
			'all':'true', 'hfPT':'', 'hfAB':'', 'hfBBT':'', 'hfPR':'','hfZ':'',
			'stadium':'','hfBBL':'','hfNewZones':'','hfGT':'R%7CPO%7CS%7C=',
			'hfSea':'','hfSit':'','player_type':'pitcher','hfOuts':'',
			'opponent':'','pitcher_throws':'','batter_stands':'','hfSA':'',
			'game_date_gt':'','game_date_lt':'','team':'','position':'','hfRO':'',
			'home_road':'','hfFlag':'','metric_1':'','hfInn':'','min_pitches':'0',
			'min_results':'0','group_by':'name','sort_col':'pitches',
			'player_event_sort':'h_launch_speed','sort_order':'desc',
			'min_abs':'0','type':'details'
		}

		#2015: April 5 - Oct 4
		#2016: April 3 - Oct 2
		#2017: April 2 - Oct 3

		self.season = {'start_dt': datetime.date(2015,4,5), 'end_dt': datetime.date(2015,10,4)}

	def get_data(self, start_dt, end_dt):

		r = requests.get(self.url.format(start_dt, end_dt), timeout=None)
		print (r.status_code)
		df = pd.read_csv(io.StringIO(r.content.decode('utf-8')), engine='c')
		return df

	def get_season_pitches(self):
		"""will query daily for each day of the season
		"""
		start_dt = self.season['start_dt']
		end_dt = self.season['end_dt']
		results = pd.DataFrame()
		for loop, i in enumerate(range(0,(end_dt - start_dt).days, 2)):
			temp_start = start_dt + datetime.timedelta(days=i)
			temp_end = temp_start + datetime.timedelta(days=1)
			#print (temp_start, temp_end)
			pitches = self.get_data(temp_start, temp_end)
			results = pd.concat([results, pitches], axis = 0)
			print (loop,temp_start, temp_end, len(results))
		results.to_csv('pitches_2015.csv', index=False)

if __name__ == '__main__':
	pitches = Statcast()
	pdf = pitches.get_season_pitches()
	#print (pdf.head())
