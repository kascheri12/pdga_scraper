import os, concurrent.futures
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
import sqlite3
from sqlite3 import Error
from time import process_time
import pandas as pd

load_dotenv()

def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_query(connection,query,args=[]):
    cursor = connection.cursor()
    try:
        cursor.execute(query, args)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

def select_query(connection,query,args=[]):
    cursor = connection.cursor()
    try:
        cursor.execute(query, args)
        return cursor.fetchall()
    except Error as e:
        print(f"The error '{e}' occurred")

def create_players_table_sql():
    return """
        CREATE TABLE IF NOT EXISTS players (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          pdga_number INTEGER,
          name TEXT,
          location TEXT,
          classification TEXT,
          member_since TEXT,
          membership_status TEXT,
          official_status TEXT,
          current_rating TEXT,
          career_events INTEGER,
          career_wins INTEGER,
          career_earnings TEXT
        );
        """

def create_pros_open_table_sql():
    return """
        CREATE TABLE IF NOT EXISTS pros_open (
            id INTEGER PRIMARY key AUTOINCREMENT,
            pdga_number INTEGER,
            name TEXT,
            rank TEXT,
            cup_rank TEXT,
            cup INTEGER,
            usdc_rank TEXT,
            usdc INTEGER,
            worlds_rank TEXT,
            worlds INTEGER,
            elite_rank TEXT,
            elite INTEGER,
            rating_rank TEXT,
            rating INTEGER,
            wins_rank TEXT,
            wins INTEGER,
            podium_rank TEXT,
            podium INTEGER,
            top10_rank TEXT,
            top10 INTEGER,
            avg DECIMAL
        );
        """

def create_pros_open_women_table_sql():
    return """
        CREATE TABLE IF NOT EXISTS pros_open_women (
            id INTEGER PRIMARY key AUTOINCREMENT,
            pdga_number INTEGER,
            name TEXT,
            rank TEXT,
            cup_rank TEXT,
            cup INTEGER,
            usdc_rank TEXT,
            usdc INTEGER,
            worlds_rank TEXT,
            worlds INTEGER,
            elite_rank TEXT,
            elite INTEGER,
            rating_rank TEXT,
            rating INTEGER,
            wins_rank TEXT,
            wins INTEGER,
            podium_rank TEXT,
            podium INTEGER,
            top10_rank TEXT,
            top10 INTEGER,
            avg DECIMAL
        );
        """

def create_players_table(connection):
    sql = create_players_table_sql()
    return execute_query(connection,sql)

def create_pros_open_table(connection):
    sql = create_pros_open_table_sql()
    return execute_query(connection,sql)

def create_pros_open_women_table(connection):
    sql = create_pros_open_women_table_sql()
    return execute_query(connection,sql)

def drop_players_sql():
    return "DROP TABLE players;"

def drop_pros_open_sql():
    return "DROP TABLE pros_open;"

def drop_pros_open_women_sql():
    return "DROP TABLE pros_open_women;"

def drop_pros_open_table(connection):
    sql = drop_pros_open_sql()
    execute_query(connection,sql)

def drop_pros_open_women_table(connection):
    sql = drop_pros_open_women_sql()
    execute_query(connection,sql)

def drop_players_table(connection):
    sql = drop_players_sql()
    execute_query(connection,sql)

def insert_players_sql():
    return """
            INSERT INTO
              players (
                pdga_number
                ,name
                ,location
                ,classification
                ,member_since
                ,membership_status
                ,official_status
                ,current_rating
                ,career_events
                ,career_wins
                ,career_earnings)
            VALUES
              ( ?,?,?,?,?,?,?,?,?,?,? );
            """

def insert_pros_open_sql():
    return """
        INSERT INTO pros_open (
            rank,
            name,
            pdga_number,
            cup_rank,
            cup,
            usdc_rank,
            usdc,
            worlds_rank,
            worlds,
            elite_rank,
            elite,
            rating_rank,
            rating,
            wins_rank,
            wins,
            podium_rank,
            podium,
            top10_rank,
            top10,
            avg)
        VALUES
          ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? );
    """

def insert_pros_open_women_sql():
    return """
        INSERT INTO pros_open_women (
            rank,
            name,
            pdga_number,
            cup_rank,
            cup,
            usdc_rank,
            usdc,
            worlds_rank,
            worlds,
            elite_rank,
            elite,
            rating_rank,
            rating,
            wins_rank,
            wins,
            podium_rank,
            podium,
            top10_rank,
            top10,
            avg)
        VALUES
          ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? );
    """

def get_highest_pdga_number_in_db(connection):
    sql = "select max(pdga_number) max_number from players;"
    rows = select_query(connection,sql)
    for r in rows:
        return r[0]

def read_player_page(driver,pdga_number):
    driver.get('https://pdga.com/player/' + str(pdga_number))
    player = {}
    player['pdga_number'] = pdga_number
    try:
        player['name'] = driver.find_elements(By.TAG_NAME,"h1")[1].text.split('#')[0].strip()
    except Exception:
        player['name'] = ''
    try:
        player['location'] = driver.find_element(By.CLASS_NAME,"location").text.split(':')[1].strip()
    except Exception:
        player['location'] = ''
    try:
        player['classification'] = driver.find_element(By.CLASS_NAME,"classification").text.split(':')[1].strip()
    except Exception:
        player['classification'] = ''
    try:
        player['member_since'] = driver.find_element(By.CLASS_NAME,"join-date").text.split(':')[1].strip()
    except Exception:
        player['member_since'] = ''
    try:
        player['membership_status'] = driver.find_element(By.CLASS_NAME,"membership-status").text.split(':')[1].strip()
    except Exception:
        player['membership_status'] = ''
    try:
        player['official_status'] = driver.find_element(By.CLASS_NAME,"official").text.split(':')[1].strip()
    except Exception:
        player['official_status'] = ''
    try:
        player['current_rating'] = driver.find_element(By.CLASS_NAME,"current-rating").text.split(':')[1].strip().split('(')[0].strip().split('+')[0].strip().split('-')[0].strip()
    except Exception:
        player['current_rating'] = -9999
    try:
        player['career_events'] = driver.find_element(By.CLASS_NAME,"career-events").text.split(':')[1].strip()
    except Exception:
        player['career_events'] = 0
    try:
        player['career_wins'] = driver.find_element(By.CLASS_NAME,"career-wins").text.split(':')[1].strip()
    except Exception:
        player['career_wins'] = 0
    try:
        player['career_earnings'] = driver.find_element(By.CLASS_NAME,"career-earnings").text.split(':')[1].strip()
    except Exception:
        player['career_earnings'] = 0
    return player

def is_real_player(player):
    # This is our qualification to be put in the db
    if player['name'] != '' and player['membership_status'] != '':
        return True
    return False

def thread_function(connection,driver,pdga_number):
    player = read_player_page(driver,pdga_number)
    if is_real_player(player):
        sql = insert_players_sql()
        args = (player['pdga_number']
                , player['name']
                , player['location']
                , player['classification']
                , player['member_since']
                , player['membership_status']
                , player['official_status']
                , player['current_rating']
                , player['career_events']
                , player['career_wins']
                , player['career_earnings'])
        execute_query(connection,sql,args)
        print(player['name'] + ' #' + str(player['pdga_number']))

def is_pro(player):
    try:
        # if pdga number exists then this is a player
        if player[1].split('#')[1].strip() != '':
            return True
    except:
        pass
    return False

def digest_pros_page(connection,driver):
    open_url = 'https://www.pdga.com/united-states-tour-ranking-open'
    driver.get(open_url)
    df = pd.read_html(driver.page_source)[0]
    players = []
    for r in df.iloc:
        if is_pro(r):
            player = {}
            player['rank'] = r[0].split(' ')[0].strip()
            player['player'] = r[1].split('#')[0].strip()
            player['pdga_number'] = r[1].split('#')[1].strip()
            player['cup_rank'] = r[2].split(' ')[0]
            player['cup'] = r[2].split(' ')[1]
            player['usdc_rank'] = r[3].split(' ')[0]
            player['usdc'] = r[3].split(' ')[1]
            player['worlds_rank'] = r[4].split(' ')[0]
            player['worlds'] = r[4].split(' ')[1]
            player['elite_rank'] = r[5].split(' ')[0]
            player['elite'] = r[5].split(' ')[1]
            player['rating_rank'] = r[6].split(' ')[0]
            player['rating'] = r[6].split(' ')[1]
            player['wins_rank'] = r[7].split(' ')[0]
            player['wins'] = r[7].split(' ')[1]
            player['podium_rank'] =  r[8].split(' ')[0]
            player['podium'] = r[8].split(' ')[1]
            player['top10_rank'] = r[9].split(' ')[0]
            player['top10'] = r[9].split(' ')[1]
            player['avg'] = r[10]
            sql = insert_pros_open_sql()
            args = (player['rank']
                    , player['player']
                    , player['pdga_number']
                    , player['cup_rank']
                    , player['cup']
                    , player['usdc_rank']
                    , player['usdc']
                    , player['worlds_rank']
                    , player['worlds']
                    , player['elite_rank']
                    , player['elite']
                    , player['rating_rank']
                    , player['rating']
                    , player['wins_rank']
                    , player['wins']
                    , player['podium_rank']
                    , player['podium']
                    , player['top10_rank']
                    , player['top10']
                    , player['avg'])
            execute_query(connection,sql,args)

            print(player['player'] + ' #' + str(player['pdga_number']))

def digest_pros_open_women_page(connection,driver):
    open_women_url = 'https://www.pdga.com/united-states-tour-ranking-open-women'
    driver.get(open_women_url)
    df = pd.read_html(driver.page_source)[0]
    players = []
    for r in df.iloc:
        if is_pro(r):
            player = {}
            player['rank'] = r[0].split(' ')[0].strip()
            player['player'] = r[1].split('#')[0].strip()
            player['pdga_number'] = r[1].split('#')[1].strip()
            player['cup_rank'] = r[2].split(' ')[0]
            player['cup'] = r[2].split(' ')[1]
            player['usdc_rank'] = r[3].split(' ')[0]
            player['usdc'] = r[3].split(' ')[1]
            player['worlds_rank'] = r[4].split(' ')[0]
            player['worlds'] = r[4].split(' ')[1]
            player['elite_rank'] = r[5].split(' ')[0]
            player['elite'] = r[5].split(' ')[1]
            player['rating_rank'] = r[6].split(' ')[0]
            player['rating'] = r[6].split(' ')[1]
            player['wins_rank'] = r[7].split(' ')[0]
            player['wins'] = r[7].split(' ')[1]
            player['podium_rank'] =  r[8].split(' ')[0]
            player['podium'] = r[8].split(' ')[1]
            player['top10_rank'] = r[9].split(' ')[0]
            player['top10'] = r[9].split(' ')[1]
            player['avg'] = r[10]
            sql = insert_pros_open_women_sql()
            args = (player['rank']
                    , player['player']
                    , player['pdga_number']
                    , player['cup_rank']
                    , player['cup']
                    , player['usdc_rank']
                    , player['usdc']
                    , player['worlds_rank']
                    , player['worlds']
                    , player['elite_rank']
                    , player['elite']
                    , player['rating_rank']
                    , player['rating']
                    , player['wins_rank']
                    , player['wins']
                    , player['podium_rank']
                    , player['podium']
                    , player['top10_rank']
                    , player['top10']
                    , player['avg'])
            execute_query(connection,sql,args)

            print(player['player'] + ' #' + str(player['pdga_number']))

if __name__ == '__main__':

    connection = create_connection(os.environ.get("DB_PATH"))

    # refresh_players_db = False
    # if refresh_players_db:
    #     drop_players_table(connection)
    #     create_players_table(connection)

    options = webdriver.ChromeOptions()
    options.binary_location = os.environ.get("CHROME_BINARY")
    options.add_argument('headless')
    chrome_driver_binary = os.environ.get("CHROME_DRIVER_BINARY_PATH")
    driver = webdriver.Chrome(chrome_driver_binary, chrome_options=options)

    # refresh_pros_open_db = True
    # if refresh_pros_open_db:
    #     drop_pros_open_table(connection)
    #     create_pros_open_table(connection)
    #
    # digest_pros_page(connection,driver)
    #
    # refresh_pros_open_women_db = True
    # if refresh_pros_open_women_db:
    #     drop_pros_open_women_table(connection)
    #     create_pros_open_women_table(connection)
    #
    # digest_pros_open_women_page(connection,driver)

    begin_number = get_highest_pdga_number_in_db(connection) + 1

    for x in range(begin_number,100000):
        thread_function(connection,driver,x)
