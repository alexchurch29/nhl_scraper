# nhl_scraper
Used to scrape and parse html and json files provided by the NHL for all games in a given date range. Returns csv files with dataframes of schedule, game rosters, shift data, game event data, coaches, and officials. 

e.g. nhl_gamescraper.games_to_csv("2017-10-07", "2017-10-10")

Builds heavily off of the NHL Scraper repository created by Harry Shomer (https://github.com/HarryShomer/Hockey-Scraper), with addtl credit to Muneeb Alam (https://github.com/muneebalam). 

Currently works for all games from the 2009 season onward. 
