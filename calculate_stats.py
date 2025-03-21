import pandas as pd

from typing import Union, List, Optional
from process_stats import process_stats
from playstats import process_playstats
from pbp_off_stats import (
    get_passing_stats,
    get_rushing_stats,
    get_receiving_stats,
    get_dropback_stats,
    get_scramble_stats,
)
from utils_date import most_recent_season
from utils_data import load_pbp
from clean_pbp import clean_pbp_data


def calculate_stats(
    seasons: Union[int, List[int]] = most_recent_season(),
    summary_level: Optional[str] = "season",
    stat_type: Optional[str] = "player",
    season_type: Optional[str] = "REG",
) -> pd.DataFrame:
    """
    Calculate comprehensive NFL statistics based on play-by-play data.
    
    This is the primary entry point for generating NFL statistics. It orchestrates the entire
    data pipeline by loading play-by-play data, processing it, calculating various statistics
    (passing, rushing, receiving, etc.), and returning a consolidated DataFrame with the results.
    
    Parameters
    ----------
    seasons : int or List[int], default=most_recent_season()
        NFL season year(s) to calculate statistics for. Can be a single season (int) 
        or multiple seasons (list of ints). Defaults to the most recent completed season.
    
    summary_level : str, default='season'
        Level at which to summarize statistics:
        - 'season': Aggregate stats for the entire season
        - 'week': Break down stats by week/game
    
    stat_type : str, default='player'
        Entity for which to calculate statistics:
        - 'player': Calculate individual player statistics
        - 'team': Calculate team-level statistics
    
    season_type : str, default='REG'
        Type of games to include in calculations:
        - 'REG': Regular season games only
        - 'POST': Postseason games only
        - 'ALL': Both regular season and postseason games
    
    Returns
    -------
    pd.DataFrame
        DataFrame containing calculated statistics with columns depending on the 
        input parameters. Will include various metrics like completions, attempts,
        yards, touchdowns, EPA, CPOE, success rate, etc., grouped by the appropriate
        level (player/team and season/week).
    
    Raises
    ------
    ValueError
        If invalid values are provided for summary_level, stat_type, or season_type.
        Also raised if filtering results in an empty DataFrame.
    
    Examples
    --------
    # Get player stats for the 2022 season
    player_season_stats = calculate_stats(2022)
    
    # Get weekly team stats for multiple seasons
    team_weekly_stats = calculate_stats(
        seasons=[2021, 2022], 
        summary_level='week', 
        stat_type='team'
    )
    """
    if isinstance(seasons, int):
        seasons = [seasons]

    if summary_level not in ["season", "week"]:
        raise ValueError("summary_level must be 'season' or 'week'")
    if stat_type not in ["player", "team"]:
        raise ValueError("stat_type must be 'player' or 'team'")
    if season_type not in ["REG", "POST", "ALL"]:
        raise ValueError("season_type must be 'REG', 'POST', or 'ALL'")

    if summary_level == "season" and stat_type == "player":
        grp_vctr = ["season", "player_id"]
    elif summary_level == "season" and stat_type == "team":
        grp_vctr = ["season", "team"]
    elif summary_level == "week" and stat_type == "player":
        grp_vctr = ["season", "week", "game_id", "player_id"]
    elif summary_level == "week" and stat_type == "team":
        grp_vctr = ["season", "week", "game_id", "team"]

    pbp = clean_pbp_data(load_pbp(seasons))

    if season_type in ["REG", "POST"] and summary_level == "season":
        pbp = pbp[pbp["season_type"] == season_type]
        if len(pbp) == 0:
            print(
                f"Warning: Filtering {seasons} data to season_type == {season_type} resulted in 0 rows. Returning empty DataFrame."
            )
            return pd.DataFrame()

    playstats = process_playstats(pbp, seasons)

    (passing_stats, rushing_stats, receiving_stats, dropback_stats, scramble_stats) = (
        get_passing_stats(pbp, grp_vctr),
        get_rushing_stats(pbp, grp_vctr),
        get_receiving_stats(pbp, grp_vctr),
        get_dropback_stats(pbp, grp_vctr),
        get_scramble_stats(pbp, grp_vctr),
    )

    stats = process_stats(
        playstats,
        grp_vctr,
        stat_type,
        summary_level,
        passing_stats,
        rushing_stats,
        receiving_stats,
        dropback_stats,
        scramble_stats,
    )

    return stats
