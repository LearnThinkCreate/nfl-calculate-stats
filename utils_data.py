import pandas as pd
import nfl_data_py as nfl

from typing import Union, List
from pathlib import Path
from utils_date import most_recent_season


def load_playstats(
    seasons: Union[int, List[int]] = most_recent_season(), update_season: bool = False
) -> pd.DataFrame:
    """
    Load play stats for a given season or list of seasons.
    """
    if isinstance(seasons, int):
        seasons = [seasons]

    # get all the files for the seasons
    files = []
    for s in seasons:
        file_path = Path("data/playstats") / f"playstats_{s}.parquet"
        files.append(_cache_playstats(s, file_path, update_season))

    # read all the files into a list of dataframes
    dfs = [pd.read_parquet(file) for file in files]

    if not dfs:
        raise ValueError(f"No play stats files found for season(s): {seasons}")

    return pd.concat(dfs)


def load_pbp(
    seasons: Union[int, List[int]] = most_recent_season(), update_season: bool = False
) -> pd.DataFrame:
    """
    Load pbp data for a given season or list of seasons.
    """
    if isinstance(seasons, int):
        seasons = [seasons]

    if min(seasons) < 1999:
        raise ValueError("PBP data only available from 1999 onwards")

    files = []
    for s in seasons:
        p = Path("data/pbp") / f"pbp_{s}.parquet"
        files.append(_cache_pbp(s, p, update_season))

    dfs = [pd.read_parquet(file) for file in files]

    return pd.concat(dfs).reset_index(drop=True)


def _cache_playstats(season: int, file_path: Path, update_season: bool = False):
    if not file_path.exists():
        playstats = nfl.import_playstats_data([season], downcast=False)
        playstats.to_parquet(file_path)
        return file_path
    elif season == most_recent_season() and update_season:
        games = nfl.import_schedules([season])
        completed_games = games.loc[games.result.notna(), "game_id"]
        playstats = pd.read_parquet(file_path)
        missing_games = playstats[~playstats.game_id.isin(completed_games)]
        if missing_games.empty:
            return file_path
        else:
            playstats = nfl.import_playstats_data([season], downcast=False)
            playstats.to_parquet(file_path)
            return file_path
    else:
        return file_path


def _cache_pbp(season: int, file_path: Path, update_season: bool = False):
    if not file_path.exists():
        pbp = nfl.import_pbp_data([season], downcast=False)
        pbp.to_parquet(file_path)
        return file_path
    elif season == most_recent_season() and update_season:
        games = nfl.import_schedules([season])
        completed_games = games.loc[games.result.notna(), "game_id"]
        pbp = pd.read_parquet(file_path)
        missing_games = pbp[~pbp.game_id.isin(completed_games)]
        if missing_games.empty:
            return file_path
        else:
            pbp = nfl.import_pbp_data([season], downcast=False)
            pbp.to_parquet(file_path)
            return file_path
    else:
        return file_path
