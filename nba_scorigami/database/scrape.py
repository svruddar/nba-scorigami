import time
from datetime import datetime
from urllib.request import urlopen

import pandas as pd
from bs4 import BeautifulSoup
from connection import create_db_connection, read_config
from mysql.connector import Error

BBAL_REF_BASE = "https://www.basketball-reference.com"
config = read_config()

# Building a list of urls for each season's data
def get_season_pages() -> list[str]:
    urls = []
    html = urlopen(BBAL_REF_BASE + "/leagues/").read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="suppress_all sortable stats_table")
    for row in table.find_all("tr"):
        header = row.find("th")
        if header.a:
            season_path = header.a.get("href").replace(".html", "_games.html")
            urls.append(BBAL_REF_BASE + season_path)
    return urls

# Building a list of urls for each month's data in a season
def get_month_pages(season_url):
    urls = []
    html = urlopen(season_url).read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    filter = soup.find("div", class_="filter")
    for link in filter.find_all("a"):
        month_path = link.get("href")
        urls.append(BBAL_REF_BASE + month_path)
    return urls


# Storing each month's games in the database
def store_month_scores(month_url, conn):
    scores_df = pd.read_html(
        month_url,
        attrs={"class": "suppress_glossary sortable stats_table"},
        flavor="bs4",
    )[0]
    for i in range(len(scores_df)):
        row = scores_df.loc[i]
        if row["PTS"] == "Playoffs":
            print(f"ROW: {row}")
        if not (
            pd.isnull(row["PTS"]) or pd.isnull(row["PTS.1"]) or row["PTS"] == "Playoffs"
        ):
            home_score, away_score = int(row["PTS"]), int(row["PTS.1"])
            key = f"{max(home_score, away_score)}:{min(home_score, away_score)}"

            cursor = conn.cursor()

            # Store key in score_pairs table
            try:
                cursor.execute("INSERT INTO score_pairs (pair) VALUES (%s)", (key,))
            except Error as err:
                print(err)

            # Get primary key of the pair from score_pairs
            try:
                cursor.execute("SELECT id FROM score_pairs WHERE pair = %s", (key,))
                score_pair_id = cursor.fetchone()[0]
            except Error as err:
                print(err)

            # Store game data in games table
            try:
                formatted_date = datetime.strptime(row["Date"], "%a, %b %d, %Y").date()
                cursor.execute(
                    "INSERT INTO games (score_pair_id, date, home_team, home_score, away_team, away_score) VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        score_pair_id,
                        formatted_date,
                        row["Home/Neutral"],
                        home_score,
                        row["Visitor/Neutral"],
                        away_score,
                    ),
                )
            except Error as err:
                print(err)

            conn.commit()


# Building the database
def main():
    conn = create_db_connection("localhost", "root", config.get("PASS"), "scores")
    for season in get_season_pages():
        for month in get_month_pages(season):
            store_month_scores(month, conn)
            time.sleep(2)
        time.sleep(5)


if __name__ == "__main__":
    main()
