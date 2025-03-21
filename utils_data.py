import pandas as pd
import nfl_data_py as nfl
import subprocess

from typing import Union, List, Optional
from pathlib import Path
from utils_date import most_recent_season


def load_playstats(
    seasons: Union[int, List[int]] = most_recent_season(), update_season: bool = False
) -> pd.DataFrame:
    """
    Load play-by-play stats for specified NFL seasons from local cache or remote source.
    
    This function retrieves detailed play statistics for the specified seasons. It first
    checks if the data is already cached locally. If not, it downloads the data from
    the NFL data repository and stores it in the local cache. For the most recent season,
    it can optionally update the data to include the latest games.
    
    Parameters
    ----------
    seasons : int or List[int], default=most_recent_season()
        NFL season year(s) to load statistics for. Can be a single season (int)
        or multiple seasons (list of ints). Defaults to the most recent completed season.
        
    update_season : bool, default=False
        If True and if the requested seasons include the current season, checks if there
        are new games that need to be added to the cache and updates accordingly.
        
    Returns
    -------
    pd.DataFrame
        Combined DataFrame containing play statistics for all requested seasons.
        Includes columns for game_id, play_id, player_id, team, stat_id, yards, etc.
        
    Raises
    ------
    ValueError
        If no play stats files are found for the requested seasons.
        
    Notes
    -----
    This function uses _cache_playstats internally to handle the caching mechanism.
    Play stats data is stored in Parquet format in the data/playstats directory.
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
    Load play-by-play data for specified NFL seasons from local cache or remote source.
    
    This function retrieves detailed play-by-play data for the specified seasons using
    the nfl_data_py package. It first checks if the data is already cached locally.
    If not, it downloads the data from the NFL data repository and stores it in the
    local cache. For the most recent season, it can optionally update the data to
    include the latest games.
    
    Parameters
    ----------
    seasons : int or List[int], default=most_recent_season()
        NFL season year(s) to load data for. Can be a single season (int)
        or multiple seasons (list of ints). Defaults to the most recent completed season.
        
    update_season : bool, default=False
        If True and if the requested seasons include the current season, checks if there
        are new games that need to be added to the cache and updates accordingly.
        
    Returns
    -------
    pd.DataFrame
        Combined DataFrame containing play-by-play data for all requested seasons.
        Includes detailed information about each play including game situation,
        players involved, play outcomes, and advanced metrics.
        
    Raises
    ------
    ValueError
        If seasons before 1999 are requested (PBP data only available from 1999 onwards).
        
    Notes
    -----
    This function uses _cache_pbp internally to handle the caching mechanism.
    Play-by-play data is stored in Parquet format in the data/pbp directory.
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
    """
    Cache play statistics data for a specific season to a local file.
    
    This function checks if play statistics for the specified season are already
    cached locally. If not, it downloads the data. For the current season,
    it can optionally check if new games have been played since the last update
    and update the cache accordingly.
    
    Parameters
    ----------
    season : int
        NFL season year to cache data for
        
    file_path : Path
        Path object pointing to where the cached file should be stored
        
    update_season : bool, default=False
        If True and if season is the current season, checks for new games
        since the last update and updates the cache if necessary
        
    Returns
    -------
    Path
        Path to the cached file (whether it was just created or already existed)
        
    Raises
    ------
    RuntimeError
        If the R script to load play stats fails
    
    Notes
    -----
    This function uses R script calls via load_playstats_cli to download and 
    convert the NFL play statistics, which are only available in RDS format.
    """
    file_pattern = str(file_path).replace(str(season), "%s")
    if not file_path.exists():
        load_playstats_cli(seasons=season, file_pattern=file_pattern)
        return file_path
    elif season == most_recent_season() and update_season:
        games = nfl.import_schedules([season])
        completed_games = games.loc[games.result.notna(), "game_id"]
        playstats = pd.read_parquet(file_path)
        missing_games = playstats[~playstats.game_id.isin(completed_games)]
        if missing_games.empty:
            return file_path
        else:
            # run the R script to load the playstats
            file_pattern = str(file_path).replace(str(season), "%s")
            result = load_playstats_cli(seasons=season, file_pattern=file_pattern)
            if result != 0:
                raise RuntimeError(f"Failed to update play stats for season {season}")
            return file_path
    else:
        return file_path


def _cache_pbp(season: int, file_path: Path, update_season: bool = False):
    """
    Cache play-by-play data for a specific season to a local file.
    
    This function checks if play-by-play data for the specified season is already
    cached locally. If not, it downloads the data using the nfl_data_py package.
    For the current season, it can optionally check if new games have been played
    since the last update and update the cache accordingly.
    
    Parameters
    ----------
    season : int
        NFL season year to cache data for
        
    file_path : Path
        Path object pointing to where the cached file should be stored
        
    update_season : bool, default=False
        If True and if season is the current season, checks for new games
        since the last update and updates the cache if necessary
        
    Returns
    -------
    Path
        Path to the cached file (whether it was just created or already existed)
    
    Notes
    -----
    This function uses nfl_data_py.import_pbp_data to download the play-by-play data
    and stores it in Parquet format for efficient storage and retrieval.
    """
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


def load_playstats_cli(
    seasons: Optional[Union[int, List[int]]] = None, file_pattern: Optional[str] = None
):
    """
    Execute an R script to load play statistics data for specified seasons.
    
    This function serves as a bridge between Python and R, allowing the Python code
    to leverage R's ability to directly read NFL play statistics data which is only
    available in RDS format. It executes an R script with the specified parameters
    to download, convert, and save the data in a format that Python can read.
    
    Parameters
    ----------
    seasons : int or List[int], optional
        NFL season year(s) to load statistics for. Can be a single season (int)
        or multiple seasons (list of ints). If None, the R script will use its
        default (which is typically the most recent season).
        
    file_pattern : str, optional
        File pattern to use for saving the data. Must contain a '%s' placeholder
        that will be replaced with the season year. If None, the R script will
        use its default pattern.
        
    Returns
    -------
    int
        Return code from the subprocess call. 0 indicates success, anything else
        indicates an error occurred.
        
    Notes
    -----
    This function executes the '11_load_playstats_cli.R' script, which in turn
    sources '10_load_playstats.R' to perform the actual data loading and conversion.
    The script output is captured and printed to standard output.
    """
    # Handle seasons input
    if seasons is not None:
        if isinstance(seasons, int):
            seasons_str = str(seasons)
        else:
            seasons_str = ",".join(map(str, seasons))
    else:
        seasons_str = None

    # Build the command
    cmd = ["Rscript", "11_load_playstats_cli.R"]  # Adjust the script name as needed
    if seasons_str is not None:
        cmd += ["--seasons", seasons_str]
    if file_pattern is not None:
        cmd += ["--file_pattern", file_pattern]

    # Execute the R script
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Display output
    print(result.stdout)
    if result.returncode != 0:
        print("Error running R script:")
        print(result.stderr)

    return result.returncode