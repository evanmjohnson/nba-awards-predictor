import pandas
import sys
from pymongo import MongoClient
from nba_py import player
import json
from StringIO import StringIO

def get_data():
  # get data for all active players in the current season
  player_list = player.PlayerList().info()

  # a list of all players who received All-NBA votes
  recieved_votes = ['James Harden', 'LeBron James', 'Russell Westbrook', 'Kawhi Leonard', 'Anthony Davis', 'Rudy Gobert', 'Stephen Curry', 'Giannis Antetokounmpo', 'Kevin Durant', 'Isaiah Thomas', 'Draymond Green', 'John Wall', 'Jimmy Butler', 'DeMar DeRozan', 'DeAndre Jordan', 'Karl-Anthony Towns', 'Chris Paul', 'Marc Gasol', 'DeMarcus Cousins', 'Paul George', 'Gordon Hayward', 'Hassan Whiteside', 'Kyrie Irving', 'Klay Thompson', 'Nikola Jokic', 'Damian Lillard', 'Paul Millsap', 'LaMarcus Aldridge', 'Blake Griffin', 'Al Horford']

  # a list of the stats columns to keep
  stats_keep = ['GP', 'W', 'W_PCT', 'MIN', 'GFM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FT_PCT', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'PF', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS', 'FT_PCT_RANK', 'REB_RANK', 'AST_RANK', 'STL_RANK', 'BLK_RANK', 'DD2', 'TD3', 'DD2_RANK', 'TD3_RANK']

  # list of # of votes given to each player in the received_votes list
  num_votes = [500, 498, 498, 490, 343, 339, 290, 258, 239, 236, 134, 125, 102, 62, 54, 50, 49, 48, 42, 40, 27, 18, 14, 14, 12, 12, 3, 1, 1, 1]

  # dictionary mapping player name to their votes received 
  votes_dict = {}
  for i in range(len(recieved_votes)):
    votes_dict[recieved_votes[i]] = num_votes[i]

  # filter out players who didn't recieve All-NBA votes
  player_list = player_list.loc[player_list['DISPLAY_FIRST_LAST'].isin(recieved_votes)]

  # MongoDB connection
  print('starting connection...')
  c = MongoClient('127.0.0.1')
  nba = c.nba # use NBA database
  nba['players'] # create a "players" collection 

  # for each player who received votes, get their stats
  for index, row in player_list.iterrows():
    player_id = row['PERSON_ID']
    player_name = row['DISPLAY_FIRST_LAST']
    yoy = player.PlayerYearOverYearSplits(player_id).by_year()
    yoy = yoy.loc[yoy['GROUP_VALUE'] == '2016-17'] # only keep 2016-2017
    yoy = yoy.filter(items = stats_keep) # filter out unwanted columns
    yoy = yoy.assign(Name = player_name) # make a new column for the players' names
    # make a column for the votes and assign it to the right player
    yoy['Votes'] = yoy['Name'].map(votes_dict)     
    records = yoy.to_dict('list') # convert to dictionary 
    # get rid of row indices in dictionary (only one row)
    records_with_strings = dict((str(k), str(records[k][0])) for k,v in records.iteritems()) 
    # insert document into players collection
    nba.players.insert_one(records_with_strings) 

def main(argc, argv):
  get_data()

if __name__ == "__main__":
  main(len(sys.argv), sys.argv)
