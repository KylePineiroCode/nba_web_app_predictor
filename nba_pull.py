import time
from datetime import datetime
import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats
from pytz import timezone

MT = timezone("America/Denver")

def current_season_str(dt=None):
    """Return NBA season label like '2025-26' based on date (MT)."""
    if dt is None:
        dt = datetime.now(MT)
    y, m = dt.year, dt.month
    start_year = y if m >= 10 else y - 1  # season starts Oct
    return f"{start_year}-{str(start_year + 1)[-2:]}"

def fetch_player_avgs(season=None, season_type="Regular Season", retries=5, pause=3):
    """Fetch season-to-date per-game averages for all players, with simple retries."""
    if season is None:
        season = current_season_str()
    for attempt in range(1, retries + 1):
        try:
            res = leaguedashplayerstats.LeagueDashPlayerStats(
                season=season,
                season_type_all_star=season_type,
                per_mode_detailed="PerGame",
                timeout=60
            )
            df = res.get_data_frames()[0]
            df.insert(0, "date_updated_mt", datetime.now(MT).strftime("%Y-%m-%d %H:%M:%S"))
            df.insert(1, "season", season)
            return df
        except Exception as e:
            if attempt == retries:
                raise
            time.sleep(pause * attempt)  

def save_csv(df, season):
    # One season file + a rolling "latest" file
    dated = datetime.now(MT).strftime("%Y%m%d")
    df.to_csv(f"nba_player_avgs_{season}_{dated}.csv", index=False)
    df.to_csv("nba_player_avgs_latest.csv", index=False)

if __name__ == "__main__":
    season = current_season_str()
    df = fetch_player_avgs(season=season, season_type="Regular Season")
    # Tidy subset of common stats
    cols = [
        "PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION", "AGE", "GP", "W", "L",
        "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV", "PF",
        "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "FTM", "FTA", "FT_PCT",
        "OREB", "DREB", "PLUS_MINUS", "PIE"
    ]
    keep = [c for c in cols if c in df.columns]
    df = df[["date_updated_mt", "season"] + keep]
    save_csv(df, season)
    print(f"Saved {len(df)} player rows for season {season}.")