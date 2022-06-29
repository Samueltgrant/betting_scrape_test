from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder, playbyplayv2
import pandas as pd
from sqlalchemy import create_engine
from db_creds import db_username, db_password, db_port, db_database, db_hostname


def merge_columns(row) -> str:
    """Takes pandas row and returns the non empty cell from HOMEDESCRIPTION, NEUTRALDESCRIPTION and VISITORDESCRIPTION,
    else returns 'No description'."""
    if row['HOMEDESCRIPTION'] not in [' ', None]:
        return row['HOMEDESCRIPTION']
    elif row['NEUTRALDESCRIPTION'] not in [' ', None]:
        return row['NEUTRALDESCRIPTION']
    elif row['VISITORDESCRIPTION'] not in [' ', None]:
        return row['VISITORDESCRIPTION']
    else:
        return 'No description'


pd.set_option('display.max_columns', None)  # allows you to see all rows when printed
output_table_name = 'last_spurs_win'

# get spurs_id using a generator expression
nba_teams = teams.get_teams()
spurs_id = next(team for team in nba_teams if team['abbreviation'] == 'SAS')['id']

# use api class to find the games spurs played
gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=spurs_id)
games = gamefinder.get_data_frames()[0]

# find GAME_ID of the last game that spurs won (already in date order).
denver_last_win = games[(games.MATCHUP.str.contains('DEN')) & (games.WL == 'W')]
denver_last_win.reset_index(drop=True, inplace=True)
game_id = denver_last_win.loc[0, 'GAME_ID']

game_df = playbyplayv2.PlayByPlayV2(game_id).get_data_frames()[0]

game_df['DESCRIPTION'] = game_df.apply(merge_columns, axis=1)
game_df = game_df[['GAME_ID', 'PERIOD', 'DESCRIPTION', 'PLAYER1_NAME', 'PLAYER1_TEAM_CITY',
                   'PLAYER2_NAME', 'PLAYER2_TEAM_CITY', 'SCORE', 'SCOREMARGIN']]

# db method 1: Postgres
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_hostname}:{db_port}/{db_database}')
game_df.to_sql(output_table_name, engine, if_exists='replace')

# connect and print out database from postgres
df_output = pd.read_sql(f"select * From {output_table_name};", engine)
print(df_output.head())

# # db method 2: sqlite3 (able to run on other computers)
# import sqlite3
# conn = sqlite3.connect('white_swan')
# c = conn.cursor()
# game_df.to_sql('last_spurs_win', conn, if_exists='replace') # create table (replace if already exists)
# conn.commit()
#
# # print results out from db as a DataFrame
# df_output = pd.DataFrame(c.execute(f'SELECT * '
#                                    f'FROM {output_table_name}'))


print(df_output.head())
