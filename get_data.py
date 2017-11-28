import sys
from pymongo import MongoClient
from nba_py import player

def get_data():
  # get data for sample player, Nene
  nene = player.get_player("Nene")
  print(nene) # prints Nene's ID
  # get Nene's year over year splits
  player_yearoveryear_splits = player.PlayerYearOverYearSplits(nene)
  
  # MongoDB connection
  c = MongoClient('127.0.0.1')
  nba = c.nba # use NBA database
  nba['players'] # create a "players" collection 
  # insert Nene's JSON into the players collection
  nba.players.insert(player_yearoveryear_splits.by_year())

def main(argc, argv):
  get_data()

if __name__ == "__main__":
  main(len(sys.argv), sys.argv)
