# nhl_scraper
Used to scrape and parse html and json files provided by the NHL for all games in a given date range. Returns pandas dataframes with  schedule, game rosters, shift data, game event data, coaches, and officials. Pd dataframes can then be converted to csv files if necessary. 

e.g. 
scrape_schedule('2017-10-01', '2018-05-01')
scrape_games_by_date('2018-10-01', '2018-01-13')
convert_to_csv()

Builds heavily off of the NHL Scraper repository created by Harry Shomer (https://github.com/HarryShomer/Hockey-Scraper), with additional credit going to Muneeb Alam (https://github.com/muneebalam). 

Currently works for all games from the 2009 season onward. 
