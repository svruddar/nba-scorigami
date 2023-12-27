import collections
from urllib.request import urlopen

import pandas as pd
from bs4 import BeautifulSoup


BBAL_REF_BASE = "https://www.basketball-reference.com"
SEASONS_URL = BBAL_REF_BASE + "/leagues/"

score_data = collections.defaultdict(list)


def get_season_pages() -> list[str]:
    urls = []
    html = urlopen(SEASONS_URL).read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="suppress_all sortable stats_table")
    for row in table.find_all("tr"):
        header = row.find("th")
        if header.a:
            season_path = header.a.get("href").replace(".html", "_games.html")
            urls.append(BBAL_REF_BASE + season_path)
    return urls


def get_season_month_pages(season_url):
    urls = []
    html = urlopen(season_url).read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    filter = soup.find("div", class_="filter")
    for link in filter.find_all("a"):
        month_path = link.get("href")
        urls.append(BBAL_REF_BASE + month_path)
    return urls


def store_month_scores(month_url):
    scores_df = pd.read_html(
        month_url,
        attrs={"class": "suppress_glossary sortable stats_table"},
        flavor="bs4",
    )[0]
    for i in range(len(scores_df)):
        row = scores_df.loc[i]
        if not (pd.isnull(row["PTS"]) or row["PTS"] == "Playoffs"):
            home_score, away_score = int(row["PTS"]), int(row["PTS.1"])
            key = f"{min(home_score, away_score)}-{max(home_score, away_score)}"
            value = {
                "date": row["Date"],
                "home team": row["Home/Neutral"],
                "home score": home_score,
                "away": row["Visitor/Neutral"],
                "away score": away_score,
            }
            score_data[key].append(value)


def main():
    seasons = get_season_pages()
    for s in seasons:
        print(s)


if __name__ == "__main__":
    main()
