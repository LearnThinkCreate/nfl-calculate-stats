import pandas as pd
import numpy as np
from functools import reduce
from utils_data import load_playstats

def process_stats(
    playstats,
    grp_vars,
    stat_type,
    summary_level,
    passing_stats_from_pbp,
    rushing_stats_from_pbp,
    receiving_stats_from_pbp,
    dropback_stats_from_pbp,
    scramble_stats_from_pbp,
):
    """
    Process football play statistics to calculate various metrics - optimized version.

    Parameters:
    -----------
    playstats : pandas.DataFrame
        DataFrame containing play-by-play statistics
    grp_vars : list
        List of column names to group by
    stat_type : str
        Type of statistics ('player' or 'team')
    summary_level : str
        Level of summary ('week' or 'season')
    passing_stats_from_pbp : pandas.DataFrame
        DataFrame with passing stats from play-by-play data
    rushing_stats_from_pbp : pandas.DataFrame
        DataFrame with rushing stats from play-by-play data
    receiving_stats_from_pbp : pandas.DataFrame
        DataFrame with receiving stats from play-by-play data
    dropback_stats_from_pbp : pandas.DataFrame
        DataFrame with dropback stats from play-by-play data
    scramble_stats_from_pbp : pandas.DataFrame
        DataFrame with scramble stats from play-by-play data
    Returns:
    --------
    pandas.DataFrame
        DataFrame with calculated statistics
    """
    # Helper function for mode calculation
    def custom_mode(series, na_rm=True):
        """Get the most common value in a series"""
        if na_rm:
            series = series.dropna()
        return (
            series.mode().iloc[0]
            if not series.empty and len(series.mode()) > 0
            else None
        )

    # Create a dictionary to hold our aggregations
    aggs = {}

    # Add player_name aggregation if needed
    if stat_type == "player":
        aggs["player_name"] = pd.NamedAgg(column="player_name", aggfunc=custom_mode)

    # Season type
    if summary_level == "week":
        aggs["season_type"] = pd.NamedAgg(column="season_type", aggfunc="first")

    # Team info
    if stat_type == "player" and summary_level == "week":
        aggs["team"] = pd.NamedAgg(column="team", aggfunc="last")

    if stat_type == 'player' and summary_level == 'season':
        aggs["team"] = pd.NamedAgg(column="team", aggfunc="first")

    # Number of games - only for season summaries
    if summary_level == "season":
        aggs["games"] = pd.NamedAgg(column="game_id", aggfunc="nunique")

    # Add basic count aggregations
    aggs.update(
        {
            # Passing stats
            "completions": pd.NamedAgg(column="is_comp", aggfunc="sum"),
            "attempts": pd.NamedAgg(column="is_att", aggfunc="sum"),
            "passing_tds": pd.NamedAgg(column="is_pass_td", aggfunc="sum"),
            "interceptions": pd.NamedAgg(column="is_int", aggfunc="sum"),
            "sacks": pd.NamedAgg(column="is_sack", aggfunc="sum"),
            "sack_fumbles": pd.NamedAgg(column="is_sack_fumble", aggfunc="sum"),
            "sack_fumbles_lost": pd.NamedAgg(
                column="is_sack_fumble_lost", aggfunc="sum"
            ),
            "passing_first_downs": pd.NamedAgg(
                column="is_pass_first_down", aggfunc="sum"
            ),
            "passing_2pt_conversions": pd.NamedAgg(column="is_pass_2pt", aggfunc="sum"),
            "qb_targets": pd.NamedAgg(column="qb_target", aggfunc="sum"),
            # Rushing stats
            "carries": pd.NamedAgg(column="is_carry", aggfunc="sum"),
            "rushing_tds": pd.NamedAgg(column="is_rush_td", aggfunc="sum"),
            "rushing_fumbles": pd.NamedAgg(column="is_rush_fumble", aggfunc="sum"),
            "rushing_fumbles_lost": pd.NamedAgg(
                column="is_rush_fumble_lost", aggfunc="sum"
            ),
            "rushing_first_downs": pd.NamedAgg(
                column="is_rush_first_down", aggfunc="sum"
            ),
            "rushing_2pt_conversions": pd.NamedAgg(column="is_rush_2pt", aggfunc="sum"),
            # Receiving stats
            "receptions": pd.NamedAgg(column="is_rec", aggfunc="sum"),
            "targets": pd.NamedAgg(column="is_target", aggfunc="sum"),
            "receiving_tds": pd.NamedAgg(column="is_rec_td", aggfunc="sum"),
            "receiving_fumbles": pd.NamedAgg(column="is_rec_fumble", aggfunc="sum"),
            "receiving_fumbles_lost": pd.NamedAgg(
                column="is_rec_fumble_lost", aggfunc="sum"
            ),
            "receiving_first_downs": pd.NamedAgg(
                column="is_rec_first_down", aggfunc="sum"
            ),
            "receiving_2pt_conversions": pd.NamedAgg(
                column="is_rec_2pt", aggfunc="sum"
            ),
            # Special teams
            "special_teams_tds": pd.NamedAgg(column="is_special_td", aggfunc="sum"),
        }
    )


    aggs.update(
        {
            "passing_yards": pd.NamedAgg(column="pass_yards", aggfunc="sum"),
            "sack_yards": pd.NamedAgg(column="sack_yards", aggfunc="sum"),
            "passing_air_yards": pd.NamedAgg(column="air_yards", aggfunc="sum"),
            "air_yards_complete_sum": pd.NamedAgg(
                column="air_yards_complete", aggfunc="sum"
            ),
            "rushing_yards": pd.NamedAgg(column="rush_yards", aggfunc="sum"),
            "receiving_yards": pd.NamedAgg(column="rec_yards", aggfunc="sum"),
            "receiving_yards_after_catch": pd.NamedAgg(column="yac", aggfunc="sum"),
        }
    )

    # Handle air yards separately for player vs team
    if stat_type == "player":
        playstats["recv_air_yards"] = np.where(playstats["is_target"], playstats["team_play_air_yards"], 0)
        aggs["receiving_air_yards"] = pd.NamedAgg(
            column="recv_air_yards", aggfunc="sum"
        )
    else:
        aggs["receiving_air_yards"] = pd.NamedAgg(column="air_yards", aggfunc="sum")

    # Store first values of team metrics for later calculations
    if stat_type == "player":
        aggs["team_targets_first"] = pd.NamedAgg(column="team_game_targets", aggfunc="first")
        aggs["team_air_yards_first"] = pd.NamedAgg(
            column="team_game_air_yards", aggfunc="first"
        )

    # Perform the aggregation
    result = playstats.groupby(grp_vars).agg(**aggs)

    # Calculate derived metrics
    # Passing yards after catch
    result["passing_yards_after_catch"] = (
        result["passing_yards"] - result["air_yards_complete_sum"]
    )
    result.drop(columns=["air_yards_complete_sum"], inplace=True)

    # QB ADOT
    result["qb_adot"] = np.where(
        result["qb_targets"] != 0,
        result["passing_air_yards"] / result["qb_targets"],
        np.nan,
    )

    # Calculate advanced metrics for player stats
    if stat_type == "player":
        # PACR
        result["pacr"] = np.where(
            result["passing_air_yards"] != 0,
            result["passing_yards"] / result["passing_air_yards"],
            np.nan,
        )

        # RACR
        result["racr"] = np.where(
            result["receiving_air_yards"] != 0,
            result["receiving_yards"] / result["receiving_air_yards"],
            np.nan,
        )

        # RECIEVER ADOT
        result["receiver_adot"] = np.where(
            result["targets"] != 0,
            result["receiving_air_yards"] / result["targets"],
            np.nan,
        )

        # For season level player stats, calculate target share and air yards share differently
        if summary_level == "season":
            # Get all game-player combinations
            player_games = playstats[
                ["season", "player_id", "team", "game_id"]
            ].drop_duplicates()

            # Get team game targets and air yards
            team_game_stats = playstats[
                [
                    "season",
                    "game_id",
                    "team",
                    "team_game_targets",
                    "team_game_air_yards",
                ]
            ].drop_duplicates()

            # Merge to get the team stats for each player-game
            player_team_stats = player_games.merge(
                team_game_stats, on=["season", "game_id", "team"], how="left"
            )

            # Sum up the team targets and air yards for each player's games
            player_team_totals = (
                player_team_stats.groupby(["season", "player_id", "team"])
                .agg({"team_game_targets": "sum", "team_game_air_yards": "sum"})
                .reset_index()
            )

            # Prepare result for merging
            result_reset = result.reset_index()

            # Merge with player_team_totals
            result_reset = result_reset.merge(
                player_team_totals, on=["season", "player_id", "team"], how="left"
            )

            # Calculate target share and air yards share with player-specific team totals
            result_reset["target_share"] = np.where(
                result_reset["team_game_targets"] > 0,
                result_reset["targets"] / result_reset["team_game_targets"],
                np.nan,
            )

            result_reset["air_yards_share"] = np.where(
                result_reset["team_game_air_yards"] > 0,
                result_reset["receiving_air_yards"]
                / result_reset["team_game_air_yards"],
                np.nan,
            )

            # Calculate WOPR using the corrected target_share and air_yards_share
            result_reset["wopr"] = np.where(
                (result_reset["team_game_targets"] > 0)
                & (result_reset["team_game_air_yards"] > 0),
                1.5 * result_reset["target_share"]
                + 0.7 * result_reset["air_yards_share"],
                np.nan,
            )

            # Clean up temporary columns
            result_reset = result_reset.drop(
                ["team_game_targets", "team_game_air_yards"], axis=1
            )

            # Convert back to the same format as before
            result = result_reset.set_index(grp_vars)
        else:
            # Original implementation for weekly stats
            # Target share
            result["target_share"] = np.where(
                result["team_targets_first"] != 0,
                result["targets"] / result["team_targets_first"],
                np.nan,
            )

            # Air yards share
            result["air_yards_share"] = np.where(
                result["team_air_yards_first"] != 0,
                result["receiving_air_yards"] / result["team_air_yards_first"],
                np.nan,
            )

            # WOPR
            result["wopr"] = np.where(
                (result["team_targets_first"] != 0)
                & (result["team_air_yards_first"] != 0),
                1.5 * (result["targets"] / result["team_targets_first"])
                + 0.7
                * (result["receiving_air_yards"] / result["team_air_yards_first"]),
                np.nan,
            )

        # Drop temporary columns used for calculations
        result = result.drop(["team_targets_first", "team_air_yards_first"], axis=1)

    # Handle season_type for season summaries
    if summary_level != "week":
        season_types = playstats.groupby(grp_vars)["season_type"].unique()
        season_types = season_types.apply(lambda x: "+".join(x))
        result["season_type"] = season_types

    # Handle opponent_team for week summaries
    if summary_level == "week":

        def compute_opponent(group):
            if group["team"].iloc[0] == group["off"].iloc[0]:
                return group["def"].iloc[0]
            else:
                return group["off"].iloc[0]

        opponents = playstats.groupby(grp_vars).apply(compute_opponent)
        result["opponent_team"] = opponents

    # Reset index to get grouping variables as columns
    result = result.reset_index()

    # Replace empty strings with NaN for all object columns
    obj_cols = result.select_dtypes(include=["object"]).columns
    result[obj_cols] = result[obj_cols].replace("", np.nan)

    # Efficient merges with multiple DataFrames
    dfs_to_merge = [result]
    if passing_stats_from_pbp is not None and not passing_stats_from_pbp.empty:
        dfs_to_merge.append(passing_stats_from_pbp)
    if rushing_stats_from_pbp is not None and not rushing_stats_from_pbp.empty:
        dfs_to_merge.append(rushing_stats_from_pbp)
    if receiving_stats_from_pbp is not None and not receiving_stats_from_pbp.empty:
        dfs_to_merge.append(receiving_stats_from_pbp)
    if dropback_stats_from_pbp is not None and not dropback_stats_from_pbp.empty:
        dfs_to_merge.append(dropback_stats_from_pbp)
    if scramble_stats_from_pbp is not None and not scramble_stats_from_pbp.empty:
        dfs_to_merge.append(scramble_stats_from_pbp)

    # Use reduce with merge for more efficient multi-merge
    if len(dfs_to_merge) > 1:
        result = reduce(
            lambda left, right: pd.merge(left, right, on=grp_vars, how="left"),
            dfs_to_merge,
        )

    # Define column names for ordering
    # First extract columns that exist in the result DataFrame
    try:
        cols_start = result.columns[
            result.columns.get_indexer(["season"])[0] : result.columns.get_indexer(
                ["passing_first_downs"]
            )[0]
            + 1
        ].tolist()
    except (KeyError, IndexError):
        # Handle the case where some columns might not exist
        cols_start = [
            col
            for col in result.columns
            if col
            in result.columns[
                result.columns.get_indexer(
                    [c for c in ["season"] if c in result.columns]
                )[0] : result.columns.get_indexer(
                    [c for c in ["passing_first_downs"] if c in result.columns]
                )[
                    0
                ]
                + 1
            ]
        ]

    # Create all column groups - handle cases where columns might not exist
    col_groups = {
        "passing_epa": [
            col
            for col in [
                "passing_epa",
                "passing_cpoe",
                "qb_adot",
                "dropback_epa",
                "dropback_success_rate",
                "epa_per_dropback",
                "scramble_epa",
                "epa_per_scramble",
                "passing_success_rate",
                "scramble_success_rate",
            ]
            if col in result.columns
        ],
        "rushing_epa": [col for col in ["rushing_epa"] if col in result.columns],
        "receiving_epa": [col for col in ["receiving_epa"] if col in result.columns],
    }

    # Only attempt to get middle columns if they exist
    try:
        col_groups["middle1"] = result.columns[
            result.columns.get_indexer(["passing_2pt_conversions"])[
                0
            ] : result.columns.get_indexer(["rushing_first_downs"])[0]
            + 1
        ].tolist()
    except (KeyError, IndexError):
        col_groups["middle1"] = []

    try:
        col_groups["middle2"] = result.columns[
            result.columns.get_indexer(["rushing_2pt_conversions"])[
                0
            ] : result.columns.get_indexer(["receiving_first_downs"])[0]
            + 1
        ].tolist()
    except (KeyError, IndexError):
        col_groups["middle2"] = []

    # Combine all column sections
    ordered_cols = []
    for group in [
        "start",
        "passing_epa",
        "middle1",
        "rushing_epa",
        "middle2",
        "receiving_epa",
    ]:
        if group in col_groups and col_groups[group]:
            ordered_cols.extend(col_groups[group])

    # Add any remaining columns
    all_specified = set(
        col for group in col_groups.values() for col in group if col in result.columns
    )
    all_specified.update(cols_start)

    remaining_cols = [col for col in result.columns if col not in all_specified]
    ordered_cols = cols_start + ordered_cols + remaining_cols

    # Remove any duplicates while preserving order
    ordered_cols = list(dict.fromkeys(ordered_cols))

    # Make sure all columns in ordered_cols actually exist in result
    ordered_cols = [col for col in ordered_cols if col in result.columns]

    # Reorder and sort
    result = result[ordered_cols].sort_values(by=grp_vars)

    return result
