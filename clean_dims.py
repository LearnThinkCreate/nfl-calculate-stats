import nfl_data_py as nfl
from utils_date import most_recent_season
import pandas as pd
from typing import Union, List


def get_teams() -> pd.DataFrame:
    """
    Retrieve and clean NFL team information for current teams.
    
    This function retrieves the team descriptive information using nfl_data_py,
    then filters it to only include the current active teams based on the 
    most recent NFL season schedule. This ensures that we only work with
    currently active franchises.
    
    Returns
    -------
    pd.DataFrame
        DataFrame containing cleaned team information with the following columns:
        - team_id: Unique identifier for each team
        - team_abbr: Team abbreviation (e.g., 'KC', 'SF')
        - team_name: Full team name (e.g., 'Kansas City Chiefs')
        - team_conf: Conference (AFC/NFC)
        - team_division: Division (East/West/North/South)
        - team_color: Primary team color
        - team_color2: Secondary team color
        - team_logo_wikipedia: URL to team logo
        - team_logo_espn: ESPN team logo URL
        And other team metadata columns.
    
    Notes
    -----
    The function specifically uses the current year's schedule to determine
    active teams, ensuring retired or moved franchises are excluded.
    """
    teams = nfl.import_team_desc()
    games = nfl.import_schedules([2024])
    teams_clean = teams[
        teams.team_abbr.isin(games.loc[games["season"] == 2024, "home_team"].unique())
    ].reset_index(drop=True)
    games = games.astype(
        {"pff": "Int64", "ftn": "Int64", "temp": "Int64", "wind": "Int64"}
    )
    return teams_clean


def get_team_abbr(abbr: str) -> str:
    """
    Standardize team abbreviations to current NFL team codes.
    
    Over time, NFL team abbreviations have changed due to team relocations,
    rebranding, or inconsistencies in data sources. This function maps
    historical or alternative team abbreviations to their current standard
    abbreviations used by the NFL.
    
    Parameters
    ----------
    abbr : str
        The original team abbreviation to be standardized
        
    Returns
    -------
    str
        The standardized team abbreviation. If the input abbreviation
        doesn't need conversion, it's returned unchanged.
    
    Examples
    --------
    >>> get_team_abbr("STL")
    "LA"
    >>> get_team_abbr("SD")
    "LAC"
    >>> get_team_abbr("NE")
    "NE"
    """
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
    return team_abbr_map.get(abbr, abbr)


def get_team_id_map() -> dict:
    """
    Create a mapping dictionary from team abbreviations to team IDs.
    
    This utility function creates a lookup dictionary that maps standard
    NFL team abbreviations to their corresponding numeric team IDs.
    This is useful for joining datasets that use different identifiers
    for teams.
    
    Returns
    -------
    dict
        Dictionary with team abbreviations as keys and team IDs as values
    
    Notes
    -----
    This function relies on get_teams() to first retrieve clean team data.
    """
    teams_clean = get_teams()
    TEAM_ID_MAP = dict(zip(teams_clean.team_abbr, teams_clean.team_id))
    return TEAM_ID_MAP


def get_games(seasons: Union[int, List[int]] = most_recent_season()) -> pd.DataFrame:
    """
    Retrieve and clean NFL game schedule data for specified seasons.
    
    This function retrieves NFL game schedules for the requested seasons
    and selects the most relevant columns for analysis and database storage.
    It provides comprehensive game metadata including teams, scores, dates,
    weather conditions, and various game identifiers.
    
    Parameters
    ----------
    seasons : int or List[int], default=most_recent_season()
        NFL season year(s) to retrieve game data for. Can be a single season (int)
        or multiple seasons (list of ints). Defaults to the most recent completed season.
        
    Returns
    -------
    pd.DataFrame
        DataFrame containing cleaned game schedule information with columns including:
        - game_id: Unique identifier for each game
        - season: NFL season year
        - game_type: Type of game (REG, POST, etc.)
        - week: NFL week number
        - gameday: Date of the game
        - home_team/away_team: Team abbreviations
        - home_score/away_score: Final scores
        And many other game metadata columns like stadium, weather, etc.
    
    Notes
    -----
    This function filters the original data to include only the most useful
    columns, as the raw NFL schedule data contains many fields not typically
    needed for analysis.
    """
    if isinstance(seasons, int):
        seasons = [seasons]
    games = nfl.import_schedules(seasons)
    games_cols = [
        "game_id",
        "season",
        "game_type",
        "week",
        "gameday",
        "weekday",
        "gametime",
        "away_team",
        "away_score",
        "home_team",
        "home_score",
        "location",
        "result",
        "total",
        "overtime",
        "gsis",
        "nfl_detail_id",
        "pfr",
        "pff",
        "espn",
        "ftn",
        "away_rest",
        "home_rest",
        "away_moneyline",
        "home_moneyline",
        "spread_line",
        "away_spread_odds",
        "home_spread_odds",
        "total_line",
        "under_odds",
        "over_odds",
        "div_game",
        "roof",
        "surface",
        "temp",
        "wind",
        "away_qb_id",
        "home_qb_id",
        "away_qb_name",
        "home_qb_name",
        "away_coach",
        "home_coach",
        "stadium_id",
        "stadium",
    ]
    games = games[games_cols]
    return games


def get_players() -> pd.DataFrame:
    """
    Retrieve and clean NFL player data.
    
    This function retrieves comprehensive information about NFL players
    using nfl_data_py, including biographical information, physical attributes,
    team affiliations, and career details. It selects the most relevant
    columns for analysis and database storage.
    
    Returns
    -------
    pd.DataFrame
        DataFrame containing cleaned player information with columns including:
        - gsis_id: Unique player identifier
        - display_name, first_name, last_name: Player name variations
        - position: Player's primary position
        - team_abbr: Current team abbreviation
        - height, weight: Physical attributes
        - college_name: Player's college
        - birth_date: Player's date of birth
        And other biographical and career information.
    
    Notes
    -----
    This function standardizes the team_abbr column using get_team_abbr()
    to ensure consistency with current NFL team abbreviations.
    """
    players = nfl.import_players()
    players = players[
        [
            "status",
            "display_name",
            "first_name",
            "last_name",
            "esb_id",
            "gsis_id",
            "birth_date",
            "college_name",
            "position",
            "jersey_number",
            "team_abbr",
            "current_team_id",
            "entry_year",
            "rookie_year",
            "draft_club",
            "college_conference",
            "status_short_description",
            "gsis_it_id",
            "short_name",
            "headshot",
            "draft_number",
            "draftround",
        ]
    ]
    nfl_ids = nfl.import_ids()[["gsis_id", "height", "weight"]]
    nfl_ids = nfl_ids.loc[~nfl_ids["gsis_id"].isna(), ["gsis_id", "height", "weight"]]
    players = players.merge(nfl_ids, on="gsis_id", how="left")
    team_id_map = get_team_id_map()
    players["team_abbr"] = players["team_abbr"].map(get_team_abbr)
    players["team_id"] = players["team_abbr"].map(team_id_map)
    players["draft_club"] = players["draft_club"].map(get_team_abbr)
    players = players.astype(
        {
            "jersey_number": "Int64",
            "height": "Int64",
            "weight": "Int64",
            "entry_year": "Int64",
            "rookie_year": "Int64",
            "gsis_it_id": "Int64",
            "draft_number": "Int64",
            "draftround": "Int64",
            "team_id": "Int64",
        }
    )
    players = players[~players["team_abbr"].isna()]
    players = players[~players["gsis_id"].isna()]
    return players


def get_snap_counts(
    seasons: Union[int, List[int]] = most_recent_season(),
) -> pd.DataFrame:
    snap_counts = nfl.import_snap_counts([seasons]).query(
        "position in ['QB', 'RB', 'WR', 'TE']"
    )[["pfr_player_id", "game_id", "season", "week", "offense_snaps", "offense_pct"]]
    nfl_ids = nfl.import_ids()[["gsis_id", "pfr_id"]]
    snap_counts = snap_counts.merge(
        nfl_ids, left_on="pfr_player_id", right_on="pfr_id", how="left"
    ).drop(columns=["pfr_player_id", "pfr_id"])
    snap_counts = snap_counts.loc[~snap_counts["gsis_id"].isna()]
    return snap_counts
