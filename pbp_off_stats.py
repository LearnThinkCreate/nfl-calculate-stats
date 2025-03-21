import numpy as np


def clean_grp_vctr(grp_vctr):
    """
    Remove player_id or team identifiers from a grouping vector.
    
    This helper function takes a list of column names used for grouping and
    removes 'player_id' and 'team' from it. This is useful when preparing
    grouping variables for aggregation functions that need to exclude these
    identifier columns.
    
    Parameters
    ----------
    grp_vctr : list
        List of column names used for grouping data
        
    Returns
    -------
    list
        Modified list with 'player_id' and 'team' columns removed
    
    Examples
    --------
    >>> clean_grp_vctr(['season', 'week', 'player_id', 'team'])
    ['season', 'week']
    """
    # remove player_id or team from grp_vctr
    return [col for col in grp_vctr if col not in ["player_id", "team"]]


def get_passing_stats(pbp, grp_vctr):
    """
    Calculate quarterback passing statistics from play-by-play data.
    
    This function processes play-by-play data to extract and aggregate passing
    statistics. It filters plays to include only passing plays and quarterback spikes,
    then calculates key passing metrics including EPA (Expected Points Added),
    CPOE (Completion Percentage Over Expected), and success rate.
    
    Parameters
    ----------
    pbp : pandas.DataFrame
        Play-by-play data containing passing plays information
        
    grp_vctr : list
        List of column names to group by when aggregating statistics
        
    Returns
    -------
    pandas.DataFrame
        DataFrame containing aggregated passing statistics with the following columns:
        - All grouping variables from grp_vctr
        - passing_epa: Sum of quarterback EPA across all passing plays
        - passing_cpoe: Mean of CPOE across all passing plays
        - passing_success_rate: Mean of success across all passing plays
    
    Notes
    -----
    This function performs the following steps:
    1. Filters the play-by-play data to include only pass plays and QB spikes
    2. Selects and renames relevant columns
    3. Groups by the specified variables
    4. Calculates aggregated statistics
    5. Resets the index to convert grouped data back to a regular DataFrame
    """
    passing_stats = (
        pbp
        # Filter rows where play_type is either "pass" or "qb_spike"
        .query('play_type in ["pass", "qb_spike"]')
        # Select and rename columns
        .loc[
            :,
            clean_grp_vctr(grp_vctr)
            + ["posteam", "passer_player_id", "qb_epa", "cpoe", "success"],
        ].rename(columns={"posteam": "team", "passer_player_id": "player_id"})
        # Group by specified columns (assuming grp_vars contains column names)
        .groupby(grp_vctr)
        # Calculate aggregates
        .agg(
            passing_epa=("qb_epa", lambda x: x.sum(skipna=True)),
            passing_cpoe=("cpoe", lambda x: np.mean(x) if x.notna().any() else np.nan),
            passing_success_rate=(
                "success",
                lambda x: np.mean(x) if x.notna().any() else np.nan,
            ),
        )
        # Reset the grouping
        .reset_index()
    )
    return passing_stats


def get_rushing_stats(pbp, grp_vctr):
    """
    Calculate player rushing statistics from play-by-play data.
    
    This function processes play-by-play data to extract and aggregate rushing
    statistics. It filters plays to include only running plays and quarterback kneels,
    then calculates rushing EPA (Expected Points Added).
    
    Parameters
    ----------
    pbp : pandas.DataFrame
        Play-by-play data containing rushing plays information
        
    grp_vctr : list
        List of column names to group by when aggregating statistics
        
    Returns
    -------
    pandas.DataFrame
        DataFrame containing aggregated rushing statistics with the following columns:
        - All grouping variables from grp_vctr
        - rushing_epa: Sum of EPA across all rushing plays
    
    Notes
    -----
    This function performs the following steps:
    1. Filters the play-by-play data to include only run plays and QB kneels
    2. Selects and renames relevant columns
    3. Groups by the specified variables
    4. Calculates the sum of EPA for rushing plays
    5. Resets the index to convert grouped data back to a regular DataFrame
    """
    return (
        pbp
        # Filter rows where play_type is either "run" or "qb_kneel"
        .query('play_type in ["run", "qb_kneel"]')
        # Select and rename columns
        .loc[
            :, clean_grp_vctr(grp_vctr) + ["posteam", "rusher_player_id", "epa"]
        ].rename(columns={"posteam": "team", "rusher_player_id": "player_id"})
        # Group by specified columns
        .groupby(grp_vctr)
        # Calculate aggregates
        .agg(rushing_epa=("epa", lambda x: x.sum(skipna=True)))
        # Reset the grouping
        .reset_index()
    )


def get_receiving_stats(pbp, grp_vctr):
    """
    Calculate player receiving statistics from play-by-play data.
    
    This function processes play-by-play data to extract and aggregate receiving
    statistics. It filters plays to include only those with a receiver identified,
    then calculates receiving EPA (Expected Points Added).
    
    Parameters
    ----------
    pbp : pandas.DataFrame
        Play-by-play data containing receiving plays information
        
    grp_vctr : list
        List of column names to group by when aggregating statistics
        
    Returns
    -------
    pandas.DataFrame
        DataFrame containing aggregated receiving statistics with the following columns:
        - All grouping variables from grp_vctr
        - receiving_epa: Sum of EPA across all receiving plays
    
    Notes
    -----
    This function performs the following steps:
    1. Filters the play-by-play data to include only plays with a receiver
    2. Selects and renames relevant columns
    3. Groups by the specified variables
    4. Calculates the sum of EPA for receiving plays
    5. Resets the index to convert grouped data back to a regular DataFrame
    """
    return (
        pbp
        # Filter rows where receiver_player_id is not null
        .query("receiver_player_id.notna()")
        # Select and rename columns
        .loc[
            :, clean_grp_vctr(grp_vctr) + ["posteam", "receiver_player_id", "epa"]
        ].rename(columns={"posteam": "team", "receiver_player_id": "player_id"})
        # Group by specified columns
        .groupby(grp_vctr)
        # Calculate aggregates
        .agg(receiving_epa=("epa", lambda x: x.sum(skipna=True)))
        # Reset the grouping
        .reset_index()
    )


def get_dropback_stats(pbp, grp_vctr):
    """
    Calculate statistics related to QB dropbacks from play-by-play data.
    
    This function processes play-by-play data to extract and aggregate statistics
    related to quarterback dropbacks. It handles both passing plays and scrambles
    that occur on dropbacks, calculating EPA and success rate metrics.
    
    Parameters
    ----------
    pbp : pandas.DataFrame
        Play-by-play data containing QB dropback information
        
    grp_vctr : list
        List of column names to group by when aggregating statistics
        
    Returns
    -------
    pandas.DataFrame
        DataFrame containing aggregated dropback statistics with the following columns:
        - All grouping variables from grp_vctr
        - dropback_epa: Sum of EPA across all dropback plays
        - dropback_success_rate: Mean success rate across all dropback plays
        - dropback_plays: Count of total dropback plays
    
    Notes
    -----
    This function handles the complexity of determining the correct player_id to use
    depending on whether the play was a pass or a scramble. For scrambles, it uses
    the rusher_player_id, and for standard dropbacks, it uses the passer_player_id.
    """
    # Determine which column to use for dropback filtering
    dropback_col = "is_dropback" if "is_dropback" in pbp.columns else "qb_dropback"

    # Filter first, then process only what we need
    result = (
        pbp.query(f"{dropback_col} == 1")
        .loc[
            :,
            clean_grp_vctr(grp_vctr)
            + [
                "posteam",
                "qb_scramble",
                "rusher_player_id",
                "passer_player_id",
                "qb_epa",
                "success",
            ],
        ]
        .assign(
            player_id=lambda df: np.where(
                df["qb_scramble"] == 1, df["rusher_player_id"], df["passer_player_id"]
            ),
            team=lambda df: df["posteam"],
        )
        .groupby(grp_vctr)
        .agg(
            dropbacks=("player_id", "count"),
            dropback_epa=("qb_epa", lambda x: x.sum(skipna=True)),
            dropback_success_rate=(
                "success",
                lambda x: np.mean(x) if x.notna().any() else np.nan,
            ),
            epa_per_dropback=(
                "qb_epa",
                lambda x: np.mean(x) if x.notna().any() else np.nan,
            ),
        )
        .reset_index()
    )

    return result


def get_scramble_stats(pbp, grp_vctr):
    """
    Calculate statistics related to QB dropbacks from play-by-play data.
    """
    result = (
        pbp.query("qb_scramble == 1")
        .loc[
            :,
            clean_grp_vctr(grp_vctr)
            + ["posteam", "rusher_player_id", "qb_epa", "success", "play_id"],
        ]
        .rename(columns={"rusher_player_id": "player_id", "posteam": "team"})
        .groupby(grp_vctr)
        .agg(
            scrambles=("play_id", "count"),
            scramble_epa=("qb_epa", lambda x: x.sum(skipna=True)),
            epa_per_scramble=("qb_epa", "mean"),
            scramble_success_rate=(
                "success",
                lambda x: np.mean(x) if x.notna().any() else np.nan,
            ),
        )
    )
    return result
