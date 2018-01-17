Used to scrape and parse html/json files provided by the NHL for all games in a given date range. Returns pandas dataframes with schedule, game rosters, shift data, pbp event data, coaches, and officials. Pd dataframes can then be converted to csv files (or otherwise) as necessary. 

e.g. from nhl_gamescraper: 
scrape_schedule('2017-10-01', '2018-05-01') 
scrape_games_by_date('2018-10-01', '2018-01-13')
convert_to_csv()

I am currently working on converting the data into a queriable SQLite database.  

The code builds heavily off of the NHL Scraper repository created by Harry Shomer (https://github.com/HarryShomer/Hockey-Scraper), with additional credit going to Muneeb Alam (https://github.com/muneebalam). I have only added my own touches where I saw fit. 

Json script files have been included in the repo as well, however, I have opted to scrape and parse the html files instead in most cases, as the json data is often unreliable on a game-by-game basis. 

The scraper should currently for all games from the 2009 season onward. 
