# betting_scrape
Task to scrape data from betting website.

There are two different python scripts in this repository: bet365_scrape.py and nba_api_scrape.py.

bet365_scrape.py uses Selenium to scrape the current premier league table from the bet365 website, along with some odds for each team to win the league, place in the top four or be relegated. I added this table to a database using sqlite.

nba_api_scrape.py uses the nba_api to gather data from an API. In this example, I filtered the games to the last game that San Antonio Spurs won vs. Denver Nuggets, and added a trimmed version of the play-by-play game statistics as a table to the sqlite database. 
