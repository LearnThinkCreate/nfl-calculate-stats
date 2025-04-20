import nfl_data_py as nfl
from utils_date import most_recent_season
import pandas as pd
from typing import Union, List


def get_teams():
    teams = nfl.import_team_desc()
    games = nfl.import_schedules([2024])
    teams_clean = teams[
        teams.team_abbr.isin(games.loc[games["season"] == 2024, "home_team"].unique())
    ].reset_index(drop=True)
    games = games.astype({
        "pff": "Int64",
        "ftn": "Int64",
        "temp": "Int64",
        "wind": "Int64"
    })
    return teams_clean

def get_team_abbr(abbr: str):
    team_map = {
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
    return team_map.get(abbr, abbr)


def get_team_id_map():
    teams_clean = get_teams()
    TEAM_ID_MAP = dict(zip(teams_clean.team_abbr, teams_clean.team_id))
    return TEAM_ID_MAP


def get_games(seasons: Union[int, List[int]] = most_recent_season()) -> pd.DataFrame:
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
    games["away_team"] = games["away_team"].map(get_team_abbr)
    games["home_team"] = games["home_team"].map(get_team_abbr)
    return games


def get_players() -> pd.DataFrame:
    players = nfl.import_players()
    players = players[[
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
    ]]
    nfl_ids =nfl.import_ids()[['gsis_id', 'height', 'weight']]
    nfl_ids = nfl_ids.loc[~nfl_ids['gsis_id'].isna(), ['gsis_id', 'height', 'weight']]
    players = players.merge(nfl_ids, on='gsis_id', how='left')     
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

def get_snap_counts(seasons: Union[int, List[int]] = most_recent_season()) -> pd.DataFrame:
    if isinstance(seasons, int):
        if seasons < 2022:
            return pd.DataFrame()
        seasons = [seasons]
    else:
        if min(seasons) < 2022 and max(seasons) > 2022:
            seasons = range(2022, max(seasons) + 1)
        else:
            return pd.DataFrame()
    snap_counts = nfl.import_snap_counts(seasons).query("position in ['QB', 'RB', 'WR', 'TE']")[['pfr_player_id', 'game_id', 'season', 'week', 'offense_snaps', 'offense_pct']]
    nfl_ids = nfl.import_ids()[['gsis_id', 'pfr_id']]
    snap_counts = snap_counts.merge(nfl_ids, left_on='pfr_player_id', right_on='pfr_id', how='left').drop(columns=['pfr_player_id', 'pfr_id'])
    snap_counts = snap_counts.loc[~snap_counts['gsis_id'].isna()]
    return snap_counts