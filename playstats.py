import pandas as pd
import numpy as np
from utils_data import load_playstats


def process_playstats(pbp, seasons, summary_level):
    """
    A comprehensive function to load, filter, and enrich play-by-play stats.
    
    This function
    1. Loads the relevant playstats data
    2. Filters it to match the pbp game_ids
    3. Computes playinfo (team and special teams info)
    4. Adds all flag columns (is_comp, is_att, etc.)
    5. Returns a fully enriched DataFrame ready for aggregation
    
    Parameters
    ----------
    pbp : pd.DataFrame
        The base play-by-play data, must contain 'game_id' column at least.
    seasons : list of int
        List of season years to load/play with.
    summary_level : str
        'week' or 'season' - determines how we eventually summarize data.
        
    Returns
    -------
    pd.DataFrame
        A fully enriched playstats DataFrame with all relevant columns
        (stat_id flags, team stats merges, etc.) ready for final aggregation.
    """        
    
    # Filter games before loading playstats
    game_ids_df = pbp[["game_id"]].drop_duplicates()
    playstats = load_playstats(seasons=seasons)
    playstats['gsis_player_id'] = playstats['gsis_player_id'].fillna('TEAM')
    # Keep only unique combinations of ["game_id", "play_id", "gsis_player_id", "stat_id"]
    playstats = playstats.drop_duplicates(subset=["game_id", "play_id", "gsis_player_id", "stat_id"])
    playstats = playstats.merge(game_ids_df, on="game_id", how="inner").rename(
        columns={"gsis_player_id": "player_id", "team_abbr": "team"}
    )
    
    bad_data, bad_seasons = find_bad_seasons(seasons)
    
    if bad_data:
        playstats["is_target"] = np.where(playstats['season'].isin(bad_seasons), playstats.stat_id.isin([21, 22, 115]), playstats.stat_id.isin([115]))
    else:
        playstats["is_target"] = (playstats["stat_id"] == 115).astype(int)
    
    # Pre-calculate derived columns using vectorized operations
    playstats["air_yards"] = playstats["yards"] * playstats["stat_id"].isin([111, 112])
    
    playstats["stat_id_str"] = playstats["stat_id"].astype(str)

    # Define groupby keys
    player_play_keys = ["season", "week", "play_id", "player_id"]
    team_play_keys = ["season", "week", "play_id", "team"]

    # Compute aggregations for each level
    # Player-level stats
    player_results = (
        playstats.groupby(player_play_keys)["stat_id_str"]
        .agg(";".join)
        .reset_index(name="more_stats")
    )
    player_results["more_stats"] = player_results["more_stats"] + ";"

    # Team-play level stats
    team_play_results = (
        playstats.groupby(team_play_keys)
        .agg({"stat_id_str": ";".join, "air_yards": "sum"})
        .reset_index()
    )
    team_play_results["stat_id_str"] = team_play_results["stat_id_str"] + ";"
    team_play_results.columns = ["season", "week", "play_id", "team", "team_stats", "team_play_air_yards"]

    # Team game level stats for player target share calculation
    team_game_keys = ["season", "game_id", "team"]
    team_game_results = (
        playstats.groupby(team_game_keys)
        .agg({"is_target": "sum", "air_yards": "sum"})
        .reset_index()
    )
    team_game_results.columns = list(team_game_keys) + [
        "team_game_targets",
        "team_game_air_yards",
    ]

    # Merge results back to original DataFrame
    result = playstats.merge(player_results, on=player_play_keys, how="left")
    result = result.merge(team_play_results, on=team_play_keys, how="left")

    # Merge game-level results for player target share calculation
    result = result.merge(team_game_results, on=team_game_keys, how="left")

    # Join with playinfo and clean up
    result = result.merge(_get_playinfo(pbp), on=["game_id", "play_id"], how="left")
    result.drop(columns=["is_target", "stat_id_str", "air_yards"], inplace=True)
    
    season_type_from_pbp = pbp[["game_id", "season_type"]].drop_duplicates()
    result = result.merge(season_type_from_pbp, on="game_id", how="left")
    
    
    # Helper function to check for IDs in arrays
    def has_id(id_value, series):
        """Check if id_value exists in elements of series"""
        # Convert id_to_check to string to ensure proper matching
        id_to_check_str = str(id_value)

        # Define a function to check if the ID exists in a single string
        def check_single_row(row_str):
            # Split the string by semicolons and check if the ID is in the resulting list
            # Using strip to handle potential whitespace
            if pd.isna(row_str):
                return False
            ids = [id_val.strip() for id_val in str(row_str).split(";")]
            return id_to_check_str in ids

        # Apply the function to each element in the series
        return series.apply(check_single_row)

    # TD IDs for reference
    td_ids_list = [11, 13, 22, 24]

    # Pre-compute flags for commonly used stat_id conditions
    # This adds columns to the DataFrame instead of creating separate masks
    result["is_comp"] = result["stat_id"].isin([15, 16])  # completions
    result["is_att"] = result["stat_id"].isin([14, 15, 16, 19])  # attempts
    result["is_pass_td"] = result["stat_id"] == 16  # passing TDs
    result["air_yards_complete"] = result["yards"] * (result["stat_id"] == 111)
    result["is_int"] = result["stat_id"] == 19  # interceptions
    result["is_sack"] = result["stat_id"] == 20  # sacks
    result["is_air_yards"] = result["stat_id"].isin([111, 112])  # air yards
    
    # result["qb_target"] = result["is_att"] & has_id(115, result["team_stats"])
    
    if bad_data:
        result["qb_target"] = np.where(result['season'].isin(bad_seasons), result["is_att"], result["is_att"] & has_id(115, result["team_stats"]))
    else:
        result["qb_target"] = result["is_att"] & has_id(115, result["team_stats"])
    
    result["is_carry"] = result["stat_id"].isin([10, 11])  # carries
    result["is_rush_yards"] = result["stat_id"].isin([10, 11, 12, 13])  # rushing yards
    result["is_rush_td"] = result["stat_id"].isin([11, 13])  # rushing TDs
    result["is_rec"] = result["stat_id"].isin([21, 22])  # receptions
    
    # result["is_target"] = result["stat_id"] == 115  # targets
    if bad_data:
        result["is_target"] = np.where(result['season'].isin(bad_seasons), result.stat_id.isin([21, 22, 115]), result.stat_id.isin([115]))
    else:
        result["is_target"] = (result["stat_id"] == 115).astype(int)
    
    result["is_rec_yards"] = result["stat_id"].isin([21, 22, 23, 24])  # receiving yards
    result["is_rec_td"] = result["stat_id"].isin([22, 24])  # receiving TDs
    result["is_yac"] = result["stat_id"] == 113
    result["is_pass_2pt"] = result["stat_id"] == 77  # passing 2pt conversions
    result["is_rush_2pt"] = result["stat_id"] == 75  # rushing 2pt conversions
    result["is_rec_2pt"] = result["stat_id"] == 104  # receiving 2pt conversions

    # Pre-compute complex flags
    result["has_fumble"] = (
        has_id(52, result["more_stats"])
        | has_id(53, result["more_stats"])
        | has_id(54, result["more_stats"])
    )
    result["has_fumble_lost"] = has_id(106, result["more_stats"])
    result["has_rush_first_down"] = has_id(3, result["team_stats"])
    result["has_pass_first_down"] = has_id(4, result["team_stats"])

    # Create combined flags
    result["is_sack_fumble"] = result["is_sack"] & result["has_fumble"]
    result["is_sack_fumble_lost"] = result["is_sack"] & result["has_fumble_lost"]
    result["is_rush_fumble"] = result["is_carry"] & result["has_fumble"]
    result["is_rush_fumble_lost"] = result["is_carry"] & result["has_fumble_lost"]
    result["is_rec_fumble"] = result["is_rec"] & result["has_fumble"]
    result["is_rec_fumble_lost"] = result["is_rec"] & result["has_fumble_lost"]
    result["is_rush_first_down"] = result["is_carry"] & result["has_rush_first_down"]
    result["is_pass_first_down"] = result["is_comp"] & result["has_pass_first_down"]
    result["is_rec_first_down"] = result["is_rec"] & result["has_pass_first_down"]
    result["is_special_td"] = (result["special"] == 1) & result["stat_id"].isin(td_ids_list)
    
    # Step 9: Add conditional sum aggregations for yards (previously in process_stats)
    # Need to use where clauses for yards
    result["pass_yards"] = np.where(result["is_comp"], result["yards"], 0)
    result["sack_yards"] = np.where(result["is_sack"], result["yards"] * -1, 0)
    result["air_yards"] = np.where(result["is_air_yards"], result["yards"], 0)
    result["rush_yards"] = np.where(result["is_rush_yards"], result["yards"], 0)
    result["rec_yards"] = np.where(result["is_rec_yards"], result["yards"], 0)
    result["yac"] = np.where(result["is_yac"], result["yards"], 0)
    
    # Step 10: Clean up temporary columns
    # We keep is_target since it's used in aggregations
    result.drop(columns=["stat_id_str"], inplace=True, errors='ignore')
    
    return result


def _get_playinfo(pbp):
    # Create a view of the data to avoid unnecessary copies
    df = pbp

    # Get the first instance of each game_id/play_id for team information
    first_records = df.drop_duplicates(["game_id", "play_id"])[
        ["game_id", "play_id", "posteam", "defteam"]
    ]

    special_plays = df[df["special"] == 1][["game_id", "play_id"]].drop_duplicates()
    special_plays["special"] = 1


    result = (
        first_records.merge(special_plays, on=["game_id", "play_id"], how="left")
        .fillna({"special": 0})
        .rename(columns={"posteam": "off", "defteam": "def"})
    )

    # Ensure 'special' is an integer column
    result["special"] = result["special"].astype(int)

    return result


def find_bad_seasons(season_list):
    # Convert the check_range to a set for faster lookup
    check_set = set(range(2003, 2009))
    
    # Find numbers that are both in the check_range and the target_list
    overlap = [num for num in season_list if num in check_set]
    
    # Return whether there's any overlap and the overlapping numbers
    return bool(overlap), overlap