import requests
import time
import numpy as np
import pandas as pd
import datetime
import io

class statsApi(object):
    """
    """
    def __init__(self):
        self.url = 'https://statsapi.mlb.com/api/v1/{0}'

    def query_standings(self, year=2018):
        endpoint = 'standings'
        params = {
            'leagueId': '103,104',
            'season': year,
            'standingTypes': 'regularSeason,springTraining,firstHalf,secondHalf',
            #'hydrate': 'division,conference,sport,league,team(nextSchedule(team,gameType=[R,F,D,L,W,C],inclusive=false),previousSchedule(team,gameType=[R,F,D,L,W,C],inclusive=true))'
            'hydrate': 'division,conference,sport,league,team'
        }
        r = requests.get(self.url.format(endpoint), params=params)
        return r.json()

    def standings(self, year=2018):
        """
        Standings Json Hierarchy (working from 1993 on):
        ['records'][division_index(0-6)]
                        --> ['standingsType']
                        --> ['division'][division_cols]
                        --> ['league'][league_cols]
                        --> ['league']['seasonDateInfo'][seasonDate_cols]
                        --> ['teamRecords'][team_index(0-5)]
                                                --> [colLowLevels]
                                                --> ['team'][team_cols]
                                                --> ['team']['venue'][venue_cols]
                                                --> ['team']['springLeague'][springLeague_cols]
                                                --> ['streak'][streak_cols]
        """

        resultSet = self.query_standings(year=year)
        colsLowLevel = [
            'team', 'season', 'streak', 'divisionRank', 'leagueRank',
            'sportRank', 'gamesPlayed', 'gamesBack', 'wildCardGamesBack',
            'leagueGamesBack', 'springLeagueGamesBack', 'sportGamesBack',
            'divisionGamesBack', 'conferenceGamesBack','lastUpdated',
            'runsAllowed', 'runsScored', 'divisionChamp', 'divisionLeader',
            'hasWildcard', 'clinched', 'eliminationNumber', 'wins',
            'losses', 'runDifferential'
        ]
        team_cols = [
            'id','locationName','abbreviation','fileCode','firstYearOfPlay',
            'shortName','teamCode','teamName','venue','springLeague','name'
        ]
        venue_cols = ['id','name']
        springLeague_cols = ['id','name','abbreviation']
        streak_cols = ['streakCode','streakNumber','streakType']
        division_cols = ['abbreviation','id','name']
        league_cols = [
            'abbreviation','conferencesInUse','divisionsInUse','id','name',
            'nameShort','numGames','numTeams','numWildcardTeams','orgCode',
            'seasonState','seasonDateInfo'
        ]
        seasonDate_cols = [
            'allStarDate','firstDate2ndHalf','lastDate1stHalf',
            'postSeasonEndDate','postSeasonStartDate','preSeasonEndDate',
            'preSeasonStartDate','regularSeasonEndDate','regularSeasonStartDate'
        ]

        standings_table = []
        for league in range(6):
            for team in range(len(resultSet['records'][league]['teamRecords'])):
                x = []
                for var in colsLowLevel:
                    if var == 'team':
                        for varTeam in team_cols:
                            if varTeam == 'venue':
                                for varTeamVenue in venue_cols:
                                    x.append(resultSet['records'][league]['teamRecords'][team][var][varTeam][varTeamVenue])
                            elif varTeam == 'springLeague':
                                for varTeamSL in springLeague_cols:
                                    x.append(resultSet['records'][league]['teamRecords'][team][var][varTeam][varTeamSL])
                            else:
                                x.append(resultSet['records'][league]['teamRecords'][team][var][varTeam])
                    elif var == 'streak':
                        for varStreak in streak_cols:
                            x.append(resultSet['records'][league]['teamRecords'][team][var][varStreak])
                    else:
                        x.append(resultSet['records'][league]['teamRecords'][team][var])
                for varDivision in division_cols:
                    x.append(resultSet['records'][league]['division'][varDivision])
                for varLeague in league_cols:
                    if varLeague == 'seasonDateInfo':
                        for varLeagueDate in seasonDate_cols:
                            try:
                                x.append(resultSet['records'][league]['league'][varLeague][varLeagueDate])
                            except:
                                x.append(None)
                    else:
                        x.append(resultSet['records'][league]['league'][varLeague])
                x.append(resultSet['records'][league]['standingsType'])

                standings_table.append(x)

        col_names = [
            'teamId', 'locationName','teamAbbr','teamFileCode','firstYearOfPlay',
            'shortName','teamCode','teamName','venueId','venueName',
            'springLeagueId','springLeagueName','springLeagueAbbr',
            'teamFullName', 'season', 'streakCode', 'streakNumber', 'streakType',
            'divisionRank', 'leagueRank', 'sportRank', 'gamesPlayed', 'gamesBack',
            'wildCardGamesBack', 'leagueGamesBack', 'springLeagueGamesBack',
            'sportGamesBack', 'divisionGamesBack', 'conferenceGamesBack',
            'lastUpdated', 'runsAllowed', 'runsScored', 'divisionChamp',
            'divisionLeader', 'hasWildcard', 'clinched', 'eliminationNumber',
            'wins', 'losses', 'runDifferential', 'divisionAbbr', 'divisionId',
            'divisionName', 'leagueAbbr', 'conferencesInUse', 'divisionsInUse',
            'leagueId', 'leagueName', 'leagueNameShort', 'numGames', 'numTeams',
            'numWildcardTeams', 'orgCode', 'seasonState', 'allStarDate',
            'firstDate2ndHalf', 'lastDate1stHalf', 'postSeasonEndDate',
            'postSeasonStartDate', 'preSeasonEndDate', 'preSeasonStartDate',
            'regularSeasonEndDate', 'regularSeasonStartDate', 'standingsType'
        ]

        standings_df = pd.DataFrame(standings_table, columns = col_names)
        return standings_df

    def save_standings(self, name, year=2018):
        standings = self.standings(year=year)
        standings.to_csv('{0}.csv'.format(name), index=False)
        return True


################################################################################
####################		Statcast_Leaderboards DATA 	########################
################################################################################
from bs4 import BeautifulSoup
import json
import re

class Statcast_Leaderboards(object):
    """Class to get Statistics Leaderboard from Baseball Savant
    TODO: Postprocessings
    """
    def __init__(self):
        self.base_url = 'https://baseballsavant.mlb.com/{0}'


    def get_js(self, endpoint, year, regex = '(?<=var data =)(.*)(?=;\n    var)', params=None, df=True):
        request = requests.get(self.base_url.format(endpoint), params=params)
#       print (request.url)
        soup = BeautifulSoup(request.text, 'html.parser')
        lists  = json.loads(re.findall(regex, soup.body.script.text)[0])
        return self.to_df(lists) if df else lists


    def to_df(self, list_dict):
        return pd.DataFrame(list_dict)


    def save_csv(self, df, name):
        df.to_csv('{0}.csv'.format(name), index=False)
        return True


    def oaa(self, year=2018):
        params = {'type':'player', 'year':year, 'min':0}
        return self.get_js(endpoint='outs_above_average', year=year, params=params)


    def doaa(self, year=2018):
        params = {'team':'', 'year':year, 'min':0}
        return self.get_js(endpoint='directional_outs_above_average', year=year, params=params)


    def poptime(self, year=2018):
        params = {'team':'', 'year':year, 'min2b':1, 'min3b':0}
        return self.get_js(endpoint='poptime', year=year, params=params)


    def sprint_speed(self, year=2018):
        params = {'position':'', 'year':year, 'team':'', 'min':5}
        return self.get_js(endpoint='sprint_speed_leaderboard', year=year, params=params)


    def cp(self, year=2018):
        params = {'type':'player', 'year':year, 'min':0}
        return self.get_js(endpoint='catch_probability_leaderboard', year=year, params=params)


    def pe_stats(self, year=2018):
        params = {'type':'pitcher', 'year':year, 'position':'','team':'','min':1}
        return self.get_js(endpoint='expected_statistics', year=year, params=params)


    def be_stats(self, year=2018):
        params = {'type':'batter', 'year':year, 'position':'','team':'','min':1}
        return self.get_js(endpoint='expected_statistics', year=year, params=params)


    def pevb(self, year=2018):
        """pitching exit velocity and barrels"""
        params = {'player_type':'pitcher', 'year':year, 'abs':0}
        return self.get_js(endpoint='statcast_leaderboard', year=year, params=params, regex  = '(?<=var leaderboard_data = )(.*)(?=;)')


    def bevb(self, year=2018):
        """batting exit velocity and barrels"""
        params = {'type':'player', 'year':year, 'min':0}
        params = {'player_type':'batter', 'year':year, 'abs':0}
        return self.get_js(endpoint='statcast_leaderboard', year=year, params=params, regex  = '(?<=var leaderboard_data = )(.*)(?=;)')

################################################################################
################################################################################


class Statcast(object):
	"""Documentation for Statcast
	"""
	def __init__(self):
		self.url = 'https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7CPO%7CS%7C=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={0}&game_date_lt={1}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details'

		self.teams = ['MIN', 'PHI', 'BAL', 'NYY', 'LAD', 'OAK', 'SEA', 'TB', 'MIL', 'MIA',
       		'KC', 'TEX', 'CHC', 'ATL', 'COL', 'HOU', 'CIN', 'LAA', 'DET', 'TOR',
       		'PIT', 'NYM', 'CLE', 'CWS', 'STL', 'WSH', 'SF', 'SD', 'BOS','ARI','FLA']

		self.url_players = 'https://baseballsavant.mlb.com/statcast_search/csv?hfSea={0}%7C&team={1}&group_by=name'

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

		self.season = {'start_dt': datetime.date(2018,3,29), 'end_dt': datetime.date(2018,10,1)}

	def get_data(self, start_dt, end_dt):

		r = requests.get(self.url.format(start_dt, end_dt), timeout=None)
		print (r.status_code)
		df = pd.read_csv(io.StringIO(r.content.decode('utf-8')), engine='c')
		return df

	def get_players_data(self, team='BOS', season = 2017):
		r = requests.get(self.url_players.format(season, team), timeout=None)
		print (r.status_code)
		try:
			df = pd.read_csv(io.StringIO(r.content.decode('utf-8')), engine='c')
			df['team'] = team
		except:
			print ('Error team {0}, season {1}'.format(team,season))
			return pd.DataFrame() #return empty df to not break loops
		return df

	def save_players(self, season=2017):
		results = pd.DataFrame()
		for team in self.teams:
			players = self.get_players_data(team=team, season=season)
			results = pd.concat([results, players], axis = 0)
			print (team, len(results))
		results.to_csv('players_{0}.csv'.format(season), index=False)


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
		results.to_csv('pitches_2018.csv', index=False)

if __name__ == '__main__':
	pitches = Statcast()
	pitches.get_season_pitches()
	#for year in range(2005,2017,1):
	#pitches.save_players(2018)
	#print (pdf.head())
