import pandas as pd


def filter_pbp_columns(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter play-by-play data to only keep essential columns for our database.

    Args:
        pbp_df (pd.DataFrame): Original play-by-play DataFrame with all columns

    Returns:
        pd.DataFrame: Filtered DataFrame with only essential columns
    """
    essential_columns = [
        # Core identifiers
        "play_id",
        "game_id",
        "season",
        "week",
        "season_type",
        # Teams
        "home_team",
        "away_team",
        "posteam",
        "defteam",
        # Game situation - timing
        "game_date",
        "qtr",
        "game_seconds_remaining",
        "half_seconds_remaining",
        "time_of_day",
        # Game situation - field position
        "down",
        "ydstogo",
        "yardline_100",
        "goal_to_go",
        # Game situation - score
        "score_differential",
        "posteam_score",
        "defteam_score",
        "total_home_score",
        "total_away_score",
        # Play classification
        "play_type",
        "shotgun",
        "no_huddle",
        "qb_dropback",
        "qb_scramble",
        "qb_kneel",
        "qb_spike",
        "pass_length",
        "pass_location",
        "run_location",
        "run_gap",
        # Performance metrics
        "epa",
        "qb_epa",
        "wp",
        "wpa",
        "air_yards",
        "yards_after_catch",
        "cpoe",
        "success",
        "xpass",
        # Outcome flags
        "first_down_rush",
        "first_down_pass",
        "first_down",
        "rush_attempt",
        "pass_attempt",
        "complete_pass",
        "incomplete_pass",
        "sack",
        "touchdown",
        "interception",
        "fumble",
        "fumble_lost",
        "pass_touchdown",
        "rush_touchdown",
        # Player data
        "passer_player_id",
        "passer_player_name",
        "passing_yards",
        "rusher_player_id",
        "rusher_player_name",
        "rushing_yards",
        "receiver_player_id",
        "receiver_player_name",
        "receiving_yards",
        # Game metadata
        "stadium",
        "roof",
        "surface",
        "temp",
        "wind",
        "div_game",
        # Special teams indicator - required by get_playinfo function
        "special",
        "special_teams_play",
    ]

    # Only keep columns that exist in the dataframe
    existing_columns = [col for col in essential_columns if col in pbp_df.columns]

    # Return filtered dataframe
    return pbp_df[existing_columns]


def normalize_team_abbreviations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize team abbreviations to current NFL team codes.

    Args:
        df (pd.DataFrame): DataFrame with team abbreviation columns

    Returns:
        pd.DataFrame: DataFrame with normalized team abbreviations
    """
    # Map of old abbreviations to current ones
    team_abbr_map = {
        "STL": "LA",
        "SD": "LAC",
        "OAK": "LV",
        "JAC": "JAX",
        "SL": "LA",
        "ARZ": "ARI",
        "BLT": "BAL",
        "CLV": "CLE",
        "HST": "HOU",
    }

    # List of columns that contain team abbreviations
    team_columns = ["home_team", "away_team", "posteam", "defteam"]

    # Create a copy to avoid modifying the original
    result = df.copy()

    # Normalize each team column
    for col in team_columns:
        if col in result.columns:
            result[col] = result[col].map(lambda x: team_abbr_map.get(x, x))

    return result


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values in the DataFrame with appropriate strategies.

    Args:
        df (pd.DataFrame): DataFrame with potential missing values

    Returns:
        pd.DataFrame: DataFrame with handled missing values
    """
    # Create a copy to avoid modifying the original
    result = df.copy()

    # For boolean/flag columns that should be 0 when missing
    flag_columns = [
        "touchdown",
        "interception",
        "sack",
        "fumble",
        "complete_pass",
        "incomplete_pass",
        "first_down",
        "first_down_rush",
        "first_down_pass",
        "rush_attempt",
        "pass_attempt",
        "qb_dropback",
        "qb_scramble",
        "qb_kneel",
        "qb_spike",
        "shotgun",
        "no_huddle",
        "fumble_lost",
        "pass_touchdown",
        "rush_touchdown",
        "success",
        "xpass",
        "special",
        "special_teams_play",  # Added for the get_playinfo function
    ]

    # Apply fill strategies
    for col in flag_columns:
        if col in result.columns:
            # Make sure we don't attempt to convert to int yet, just fill NaN with 0
            result[col] = result[col].fillna(0)

    # Handle special case for score_differential
    if "score_differential" in result.columns:
        # Fill with 0 (tie game) for missing values
        result["score_differential"] = result["score_differential"].fillna(0)

    # Handle other specific columns
    if "yardline_100" in result.columns:
        # For missing yardline values, assume midfield (50)
        result["yardline_100"] = result["yardline_100"].fillna(50)

    # Handle missing player IDs with empty strings rather than nulls
    id_columns = [
        col
        for col in result.columns
        if col.endswith("_id") or col.endswith("_player_id")
    ]
    for col in id_columns:
        result[col] = result[col].fillna("")

    return result


def add_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived fields that are useful for filtering and analysis.

    Args:
        df (pd.DataFrame): Original DataFrame

    Returns:
        pd.DataFrame: DataFrame with added derived fields
    """
    # Create a copy to avoid modifying the original
    result = df.copy()

    # Safely handle potentially missing columns
    if "score_differential" in result.columns:
        # Handle NaN values before conversion
        score_diff = result["score_differential"].fillna(0)
        # Trailing/leading indicators
        result["is_trailing"] = (score_diff < 0).astype(int)
        result["is_leading"] = (score_diff > 0).astype(int)

    # Red zone indicator
    if "yardline_100" in result.columns:
        yardline = result["yardline_100"].fillna(100)
        result["is_red_zone"] = (yardline <= 20).astype(int)

    # Early/late down indicators
    if "down" in result.columns:
        down_values = result["down"].fillna(0)
        result["is_early_down"] = (down_values <= 2).astype(int)
        result["is_late_down"] = (down_values >= 3).astype(int)

    # Likely pass indicator
    if "xpass" in result.columns:
        xpass_values = result["xpass"].fillna(0)
        result["is_likely_pass"] = (xpass_values >= 0.5).astype(int)

    # Dropback indicator (including sacks, scrambles, and regular passes)
    if "qb_dropback" in result.columns:
        result["is_dropback"] = result["qb_dropback"].fillna(0).astype(int)

    # Play success indicator
    if "success" in result.columns:
        result["is_success"] = result["success"].fillna(0).astype(int)

    return result


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert data types to optimized formats for database storage.

    Args:
        df (pd.DataFrame): DataFrame with default data types

    Returns:
        pd.DataFrame: DataFrame with optimized data types
    """
    # Create a copy to avoid modifying the original
    result = df.copy()

    # Integer columns
    int_columns = [
        "season",
        "week",
        "down",
        "qtr",
        "goal_to_go",
        "touchdown",
        "interception",
        "sack",
        "fumble",
        "complete_pass",
        "incomplete_pass",
        "first_down",
        "rush_attempt",
        "pass_attempt",
    ]

    # Float columns that should remain float
    float_columns = [
        "epa",
        "qb_epa",
        "wp",
        "wpa",
        "cpoe",
        "xpass",
        "yardline_100",
        "score_differential",
    ]

    # Boolean columns (0/1)
    bool_columns = [
        "shotgun",
        "no_huddle",
        "qb_dropback",
        "qb_scramble",
        "qb_kneel",
        "qb_spike",
        "success",
        "fumble_lost",
        "first_down_rush",
        "first_down_pass",
    ]

    # Apply conversions with careful handling of NaN values
    for col in int_columns:
        if col in result.columns:
            # Handle NaN values by filling with 0 before conversion to int
            result[col] = (
                pd.to_numeric(result[col], errors="coerce").fillna(0).astype(int)
            )

    for col in float_columns:
        if col in result.columns:
            # For float columns, we can allow NaN values to remain
            result[col] = pd.to_numeric(result[col], errors="coerce")

    for col in bool_columns:
        if col in result.columns:
            # Handle NaN values before converting to int
            result[col] = (
                pd.to_numeric(result[col], errors="coerce").fillna(0).astype(int)
            )

    # Special handling for flag/indicator columns
    # Make sure any derived columns are also properly converted
    derived_cols = [col for col in result.columns if col.startswith("is_")]
    for col in derived_cols:
        if col not in bool_columns and col in result.columns:
            result[col] = result[col].fillna(0).astype(int)

    return result


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate data for integrity and consistency.
    Remove or fix invalid records.

    Args:
        df (pd.DataFrame): DataFrame to validate

    Returns:
        pd.DataFrame: Validated DataFrame
    """
    # Create a copy to avoid modifying the original
    result = df.copy()

    # Remove rows with missing critical fields
    critical_fields = ["play_id", "game_id", "season"]
    result = result.dropna(subset=critical_fields)

    # Ensure play_id and game_id are properly formatted
    if "play_id" in result.columns:
        # Ensure play_id is numeric
        result = result[pd.to_numeric(result["play_id"], errors="coerce").notna()]

    # Validate numeric ranges
    if "down" in result.columns:
        # Down must be 1-4
        result = result[
            (result["down"].isna()) | ((result["down"] >= 1) & (result["down"] <= 4))
        ]

    # Only include standard play types
    if "play_type" in result.columns:
        valid_play_types = [
            "pass",
            "run",
            "punt",
            "field_goal",
            "kickoff",
            "extra_point",
            "qb_kneel",
            "qb_spike",
            "no_play",
        ]
        result = result[
            (result["play_type"].isna()) | (result["play_type"].isin(valid_play_types))
        ]

    return result


def clean_pbp_data(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Master function to clean play-by-play data.
    Applies all cleaning steps in sequence.

    Args:
        pbp_df (pd.DataFrame): Raw play-by-play DataFrame

    Returns:
        pd.DataFrame: Clean, filtered, and enhanced DataFrame ready for database
    """
    # Apply all cleaning steps in sequence
    cleaned_df = (
        pbp_df.pipe(filter_pbp_columns)
        .pipe(normalize_team_abbreviations)
        .pipe(handle_missing_values)
        .pipe(add_derived_fields)
        .pipe(convert_data_types)
        .pipe(validate_data)
    )

    return cleaned_df
