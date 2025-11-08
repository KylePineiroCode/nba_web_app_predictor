import os
import time
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from nba_api.stats.endpoints import leaguedashplayerstats
from nba_api.stats.library.http import NBAStatsHTTP
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Output dir (relative to repo root) ---
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

def _prepare_session():
    """Strengthen the NBA API HTTP session with headers + retries."""
    http = NBAStatsHTTP()
    s = http.get_session()

    # Pretend to be a browser; stats.nba.com is picky
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/129.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.nba.com/stats",
        "Origin": "https://www.nba.com",
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
    })

    # Retry on rate limits / transient network errors
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=2,                 # 0, 2, 4, 8, 16s ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return http

def fetch_player_avgs(season: str, season_type: str = "Regular Season") -> pd.DataFrame:
    """
    Pull per-game (season-to-date) averages for every player.
    season format examples: '2024-25', '2023-24'
    """
    _ = _prepare_session()
    # timeout helps in CI; PerGame returns per-game averages
    res = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        season_type_all_star=season_type,
        per_mode_detailed="PerGame",
        timeout=120,  # seconds
    )
    df = res.get_data_frames()[0]

    # Common Columns
    cols = [
        "PLAYER_ID","PLAYER_NAME","TEAM_ID","TEAM_ABBREVIATION","GP",
        "MIN","PTS","REB","AST","STL","BLK","TOV","FG_PCT","FG3_PCT","FT_PCT","PLUS_MINUS"
    ]
    df = df[[c for c in cols if c in df.columns]].copy()
    return df

if __name__ == "__main__":
    # Season auto-detect (simple): if before Aug -> same season label, else roll to next
    today = datetime.now(timezone.utc)
    year = today.year
    # NBA season rolls around fall; adjust if you want to be more exact
    season_label = f"{year-1}-{str(year)[-2:]}" if today.month < 8 else f"{year}-{str(year+1)[-2:]}"

    # Be nice to the API
    try:
        df = fetch_player_avgs(season=season_label, season_type="Regular Season")
    except Exception as e:
        # One more short backoff try in case of network noise
        time.sleep(5)
        df = fetch_player_avgs(season=season_label, season_type="Regular Season")

    # Save outputs
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    dated_path = DATA_DIR / f"nba_player_avgs_{stamp}.csv"
    latest_path = DATA_DIR / "nba_player_avgs_latest.csv"

    df.to_csv(dated_path, index=False)
    df.to_csv(latest_path, index=False)

    print(f"Wrote {dated_path}")
    print(f"Wrote {latest_path}")
