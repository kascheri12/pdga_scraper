import os, concurrent.futures
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
import sqlite3
from sqlite3 import Error
from time import process_time

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

def create_players_table(connection):
    sql = create_players_table_sql()
    return execute_query(connection,sql)


def drop_players_sql():
    return "DROP TABLE players;"

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


def thread_function(connection,driver,pdga_number):

    player = read_player_page(driver,pdga_number)
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

if __name__ == '__main__':

    connection = create_connection(os.environ.get("DB_PATH"))

    refresh_players_db = False
    if refresh_players_db:
        drop_players_table(connection)
        create_players_table(connection)

    options = webdriver.ChromeOptions()
    options.binary_location = os.environ.get("CHROME_BINARY")
    options.add_argument('headless')
    chrome_driver_binary = os.environ.get("CHROME_DRIVER_BINARY_PATH")
    driver = webdriver.Chrome(chrome_driver_binary, chrome_options=options)

    for x in range(1,100000):
        thread_function(connection,driver,x)
