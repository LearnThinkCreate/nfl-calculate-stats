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

    playstats = process_playstats(pbp, seasons, summary_level)

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
