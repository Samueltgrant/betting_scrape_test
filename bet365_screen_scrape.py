from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import pandas as pd
from db_creds import db_username, db_password, db_port, db_database, db_hostname
from sqlalchemy import create_engine
import psycopg2


URL = r'https://www.bet365.com/#/AC/B1/C1/D1002/E76169570/G40/K^4/'
SERVICE = Service(r"C:\Users\samgr\Drivers\chromedriver.exe")  # set this to path of local chromedriver.exe
output_table_name = 'premier_league_table'
# note - chrome driver will probably be different if this is run on another computer (dependent on chrome version)
options = webdriver.ChromeOptions()
options.add_argument(r"C:\Users\Terrafirma\Desktop\UserData")
from webdriver_manager.chrome import ChromeDriverManager


with webdriver.Chrome(ChromeDriverManager().install()) as driver:  # uses context manager to always close browser
    driver.get(URL)  # loads url in a new browser
    driver.implicitly_wait(10)  # waits for up to 10 seconds for page to load.
    to_win = driver.find_element(By.CLASS_NAME, 'pml-MarketLeagueOdds-col1').text.split('\n')  # odds to win league
    top_four = driver.find_element(By.CLASS_NAME, 'pml-MarketLeagueOdds-col2').text.split('\n')  # odds to get top 4
    relegated = driver.find_element(By.CLASS_NAME, 'pml-MarketLeagueOdds-col3').text.split('\n')  # odds for relegation

    team_stats = [team.text for team in driver.find_elements(By.CLASS_NAME, 'pml-ParticipantLeagueStatistics')]

    team_stats_dict = dict()
    for table_position, stats in enumerate(team_stats, start=1):
        team_name, played, win, drawn, loss, f, diff, pts = stats.split("\n")

        # team pos. and team name is joined: below team pos. from string. Slicing is inefficient but ok in this case.
        team_name = ''.join([char for char in team_name if not char.isdigit()])

        # convert data to dictionary of dictionaries - faster than iterating through a DataFrame
        team_stats_dict[table_position] = {'team': team_name,
                                           'played': int(played),
                                           'win': int(win),
                                           'drawn': int(drawn),
                                           'loss': int(loss),
                                           'for': int(f),
                                           'diff': int(diff),
                                           'pts': int(pts),

                                           'odds_win': float(to_win[table_position]),
                                           'odds_top_four': float(top_four[table_position]),
                                           'odds_relegated': float(relegated[table_position])}

    # convert data to a DataFrame
    team_df = pd.DataFrame.from_dict(team_stats_dict, orient='index')


# db method 1: postgres
# postgres is a better solution in a production environment so have created a locally hosted postgres
# solution. This won't run on another machine so will either need to be configured or use method 2 below.
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_hostname}:{db_port}/{db_database}')
team_df.to_sql(output_table_name, engine, if_exists='replace')  # connected using sqlqlchemy

# table manipulation using psycopg2
con = psycopg2.connect(host=db_hostname,
                       port=db_port,
                       database=db_database,
                       user=db_username,
                       password=db_password)

cur = con.cursor()
cur.execute(f"ALTER TABLE {output_table_name} "
            f"ADD COLUMN source varchar(100) DEFAULT '{URL}';")
con.commit()

# select all data
select_query = cur.execute(f"select * From {output_table_name};")
database_return = cur.fetchall()
print(database_return)


# # db method 2: sqlite3
# # to use postgres across multiple computers, it must be hosted on aws or another server.
# # I've added in a short bit of code below using sqlite3 to create a local db so the code
# # can be run by the user of this code. - simply comment out method 1 instead.
# import sqlite3
# conn = sqlite3.connect('white_swan') # add data to a sqlite database (called 'white_swan').
# c = conn.cursor()
# team_df.to_sql('premier_league_table', conn, if_exists='replace')  # create table (replace if already exists)
# conn.commit()
#
# # print results out from db using SQL
# c.execute('SELECT * '
#           'FROM premier_league_table')
#
# print(c.fetchall())
