Used to scrape and parse html/json files provided by the NHL for all games in a given date range. Returns pandas dataframes with schedule, game rosters, shift data, pbp event data, coaches, and officials. Pd dataframes can then be converted to sql db files for further querying. 

The code borrows heavily from the NHL Scraper repository created by Harry Shomer (https://github.com/HarryShomer/Hockey-Scraper), with additional credit going to Muneeb Alam (https://github.com/muneebalam).  

Json script files have been included in the repo as well, however, I have opted to scrape and parse the html files instead in most cases, as the json data seems to be unreliable on a game-by-game basis. 

The scraper should currently for all games from the 2009 season onward. Looking to add 2007/08 compatibility down the road. 
