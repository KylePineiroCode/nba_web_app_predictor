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
    # More aggressive retry settings
    retry = Retry(
        total=8,  # Increased from 5
        read=8,
        connect=8,
        backoff_factor=3,  # Increased from 2              
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
    
    res = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        season_type_all_star=season_type,
        per_mode_detailed="PerGame",
        timeout=180,
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
    # Current season: 2025-26
    season_label = "2025-26"
    
    print(f"Fetching data for season: {season_label}")
    print(f"Current time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    
    time.sleep(8)
    
    max_attempts = 4 
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"Attempt {attempt}/{max_attempts}...")
            df = fetch_player_avgs(season=season_label, season_type="Regular Season")
            print(f"Successfully fetched data!")
            break
        except Exception as e:
            error_msg = str(e)
            print(f"Attempt {attempt} failed: {error_msg[:200]}...")
            if attempt < max_attempts:
                # Progressive backoff: 20s, 40s, 60s
                wait_time = 20 * attempt
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"All {max_attempts} attempts failed after {sum(20*i for i in range(1, max_attempts))} seconds of retries")
                raise
    
    # Save outputs
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    dated_path = DATA_DIR / f"nba_player_avgs_{stamp}.csv"
    latest_path = DATA_DIR / "nba_player_avgs_latest.csv"
    
    # Save both files
    df.to_csv(dated_path, index=False)
    df.to_csv(latest_path, index=False)
    
    print(f"Wrote {dated_path} (local only - NOT pushed to Git)")
    print(f"Wrote {latest_path} (will be pushed to Git)")
    print(f"Fetched data for {len(df)} players in season {season_label}")