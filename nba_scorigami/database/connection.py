import configparser
import os

import mysql.connector
from mysql.connector import Error

CONFIG_FILE = "db.config"


# Getting config information
def read_config():
    curr_path = os.path.abspath(__file__)
    config_path = os.path.join(os.path.dirname(curr_path), CONFIG_FILE)

    config = configparser.ConfigParser()
    config.read(config_path)
    return config["Credentials"]


# Creating database connection
def create_db_connection(host_name, user_name, user_password, db):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name, user=user_name, passwd=user_password, database=db
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


"""
CREATE TABLE score_pairs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    pair VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE games (
    id INT AUTO_INCREMENT PRIMARY KEY,
    score_pair_id INT,
    date DATE,
    home_team VARCHAR(255),
    home_score INT,
    away_team VARCHAR(255),
    away_score INT,
    FOREIGN KEY (score_pair_id) REFERENCES score_pairs(id) ON DELETE CASCADE,
    UNIQUE KEY identifier (date, home_team, away_team)
);
"""
