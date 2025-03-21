import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine
import time
from utils_data import load_pbp
from clean_pbp import clean_pbp_data
from playstats import process_playstats
from clean_dims import get_games, get_players, get_teams, get_snap_counts
from dotenv import load_dotenv
import os

load_dotenv()


def seed_database(
    db_name="nfl_stats", user="xxxx", password="xxxx", host="localhost", port="5432"
):
    """
    Seed the NFL stats database with teams, players, games, plays, and playstats data.
    
    This function serves as the main entry point for populating a PostgreSQL database
    with NFL statistics data. It orchestrates the entire ETL (Extract, Transform, Load)
    process by:
    
    1. Establishing connections to the database using both SQLAlchemy and psycopg2
    2. Extracting data from various sources using utility functions:
       - Team information (get_teams)
       - Player information (get_players)
       - Game schedules (get_games)
       - Player snap counts (get_snap_counts)
       - Play-by-play data (load_pbp, clean_pbp_data)
       - Play statistics (process_playstats)
    3. Transforming the data as needed (cleaning, filtering, etc.)
    4. Loading the data into the appropriate database tables using a mix of:
       - SQLAlchemy's to_sql for smaller tables
       - Psycopg2's execute_batch for larger tables and better performance
    
    The function implements proper transaction handling with commit/rollback
    to ensure database integrity.
    
    Parameters
    ----------
    db_name : str, default='nfl_stats'
        Name of the PostgreSQL database to connect to
        
    user : str, default='xxxx'
        Database username for authentication
        
    password : str, default='xxxx'
        Database password for authentication
        
    host : str, default='localhost'
        Database server hostname or IP address
        
    port : str, default='5432'
        Database server port number
        
    Returns
    -------
    None
        The function operates via side effects (populating database tables)
        
    Notes
    -----
    - This function uses both SQLAlchemy and psycopg2 connections:
      - SQLAlchemy for simpler to_sql operations on smaller tables
      - Psycopg2 with execute_batch for better performance on larger tables
    - The database schema should already exist before calling this function
    - Error handling is implemented to roll back transactions on failure
    - Real passwords should be stored in environment variables (.env file)
      rather than hardcoded in the function parameters
    """
    # Create database connection
    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(conn_string)
    conn = psycopg2.connect(
        dbname=db_name, user=user, password=password, host=host, port=port
    )
    conn.autocommit = False

    try:
        print("Fetching data...")
        teams = get_teams()
        players = get_players()
        games = get_games([2024])  # Only current season
        snap_counts = get_snap_counts(2024)  # Get snap counts for current season

        print("Loading play-by-play data...")
        pbp_data = clean_pbp_data(load_pbp([2024]))  # Only current season

        print("Processing playstats...")
        playstats = process_playstats(pbp_data, [2024], "week")
        playstats["special"] = playstats["special"].fillna(0)

        # Start transaction
        print("Beginning database import...")

        # Insert teams (small table, use to_sql)
        print("Importing teams...")
        teams.to_sql("teams", engine, if_exists="append", index=False)

        # Insert players (larger table, use batch insert)
        print("Importing players...")
        cursor = conn.cursor()

        # Prepare data for batch insert
        player_values = []
        for _, row in players.iterrows():
            player_values.append(
                (
                    row.get("gsis_id", None),
                    row.get("status", None),
                    row.get("display_name", None),
                    row.get("first_name", None),
                    row.get("last_name", None),
                    row.get("esb_id", None),
                    row.get("birth_date", None),
                    row.get("college_name", None),
                    row.get("position", None),
                    (
                        None
                        if pd.isna(row.get("jersey_number"))
                        else int(row.get("jersey_number"))
                    ),
                    None if pd.isna(row.get("height")) else int(row.get("height")),
                    None if pd.isna(row.get("weight")) else int(row.get("weight")),
                    row.get("team_abbr", None),
                    row.get("current_team_id", None),
                    (
                        None
                        if pd.isna(row.get("entry_year"))
                        else int(row.get("entry_year"))
                    ),
                    (
                        None
                        if pd.isna(row.get("rookie_year"))
                        else int(row.get("rookie_year"))
                    ),
                    row.get("draft_club", None),
                    row.get("college_conference", None),
                    row.get("status_short_description", None),
                    (
                        None
                        if pd.isna(row.get("gsis_it_id"))
                        else int(row.get("gsis_it_id"))
                    ),
                    row.get("short_name", None),
                    row.get("headshot", None),
                    (
                        None
                        if pd.isna(row.get("draft_number"))
                        else int(row.get("draft_number"))
                    ),
                    (
                        None
                        if pd.isna(row.get("draftround"))
                        else int(row.get("draftround"))
                    ),
                )
            )

        print(f"Inserting {len(player_values)} players...")
        player_insert_query = """
        INSERT INTO players 
        (gsis_id, status, display_name, first_name, last_name, esb_id, birth_date, 
         college_name, position, jersey_number, height, weight, team_abbr, 
         current_team_id, entry_year, rookie_year, draft_club, college_conference, 
         status_short_description, gsis_it_id, short_name, headshot, draft_number, draftround)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (gsis_id) DO UPDATE SET 
        status = EXCLUDED.status,
        team_abbr = EXCLUDED.team_abbr,
        updated_at = NOW()
        """
        execute_batch(cursor, player_insert_query, player_values, page_size=1000)

        # Insert games
        print("Importing games...")
        games_values = []
        for _, row in games.iterrows():
            games_values.append(
                (
                    row.get("game_id", None),
                    row.get("season", None),
                    row.get("game_type", None),
                    row.get("week", None),
                    row.get("gameday", None),
                    row.get("weekday", None),
                    row.get("gametime", None),
                    row.get("away_team", None),
                    row.get("away_score", None),
                    row.get("home_team", None),
                    row.get("home_score", None),
                    row.get("location", None),
                    row.get("result", None),
                    row.get("total", None),
                    row.get("overtime", None),
                    row.get("gsis", None),
                    row.get("nfl_detail_id", None),
                    row.get("pfr", None),
                    None if pd.isna(row.get("pff")) else int(row.get("pff")),
                    row.get("espn", None),
                    None if pd.isna(row.get("ftn")) else int(row.get("ftn")),
                    row.get("away_rest", None),
                    row.get("home_rest", None),
                    row.get("away_moneyline", None),
                    row.get("home_moneyline", None),
                    row.get("spread_line", None),
                    row.get("away_spread_odds", None),
                    row.get("home_spread_odds", None),
                    row.get("total_line", None),
                    row.get("under_odds", None),
                    row.get("over_odds", None),
                    row.get("div_game", None),
                    row.get("roof", None),
                    row.get("surface", None),
                    None if pd.isna(row.get("temp")) else int(row.get("temp")),
                    None if pd.isna(row.get("wind")) else int(row.get("wind")),
                    row.get("away_qb_id", None),
                    row.get("home_qb_id", None),
                    row.get("away_qb_name", None),
                    row.get("home_qb_name", None),
                    row.get("away_coach", None),
                    row.get("home_coach", None),
                    row.get("stadium_id", None),
                    row.get("stadium", None),
                )
            )

        games_insert_query = """
        INSERT INTO games
        (game_id, season, game_type, week, gameday, weekday, gametime, 
         away_team, away_score, home_team, home_score, location, result, 
         total, overtime, gsis, nfl_detail_id, pfr, pff, espn, ftn, 
         away_rest, home_rest, away_moneyline, home_moneyline, spread_line, 
         away_spread_odds, home_spread_odds, total_line, under_odds, over_odds, 
         div_game, roof, surface, temp, wind, away_qb_id, home_qb_id, 
         away_qb_name, home_qb_name, away_coach, home_coach, stadium_id, stadium)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id) DO UPDATE SET
        away_score = EXCLUDED.away_score,
        home_score = EXCLUDED.home_score,
        updated_at = NOW()
        """
        execute_batch(cursor, games_insert_query, games_values, page_size=100)

        # Insert snap counts
        print("Importing snap counts...")
        snap_counts_values = []
        for _, row in snap_counts.iterrows():
            snap_counts_values.append(
                (
                    row.get("gsis_id", None),
                    row.get("game_id", None),
                    row.get("season", None),
                    row.get("week", None),
                    (
                        None
                        if pd.isna(row.get("offense_snaps"))
                        else int(row.get("offense_snaps"))
                    ),
                    (
                        None
                        if pd.isna(row.get("offense_pct"))
                        else float(row.get("offense_pct"))
                    ),
                )
            )

        snap_counts_insert_query = """
        INSERT INTO snap_counts
        (gsis_id, game_id, season, week, offense_snaps, offense_pct)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (gsis_id, game_id) DO UPDATE SET
        offense_snaps = EXCLUDED.offense_snaps,
        offense_pct = EXCLUDED.offense_pct,
        updated_at = NOW()
        """
        print(f"Inserting {len(snap_counts_values)} snap count records...")
        execute_batch(
            cursor, snap_counts_insert_query, snap_counts_values, page_size=1000
        )

        # Insert plays
        print("Importing plays...")
        plays_columns = list(pbp_data.columns)
        placeholders = ", ".join(["%s"] * len(plays_columns))
        plays_insert_query = f"""
        INSERT INTO plays
        ({", ".join(plays_columns)})
        VALUES ({placeholders})
        ON CONFLICT (season, play_id, game_id) DO UPDATE SET
        updated_at = NOW()
        """

        # Process in batches to prevent memory issues
        batch_size = 5000
        total_plays = len(pbp_data)
        for i in range(0, total_plays, batch_size):
            batch = pbp_data.iloc[i : i + batch_size]
            values = [tuple(row) for row in batch.values]
            print(
                f"Inserting plays batch {i//batch_size + 1}/{(total_plays//batch_size) + 1}..."
            )
            execute_batch(cursor, plays_insert_query, values, page_size=1000)

        # Insert playstats
        print("Importing playstats...")
        playstats_columns = list(playstats.columns)
        placeholders = ", ".join(["%s"] * len(playstats_columns))
        playstats_insert_query = f"""
        INSERT INTO playstats
        ({", ".join(playstats_columns)})
        VALUES ({placeholders})
        ON CONFLICT (season, play_id, game_id, player_id, stat_id) DO UPDATE SET
        updated_at = NOW()
        """

        # Process in batches
        total_playstats = len(playstats)
        for i in range(0, total_playstats, batch_size):
            batch = playstats.iloc[i : i + batch_size]
            values = [tuple(row) for row in batch.values]
            print(
                f"Inserting playstats batch {i//batch_size + 1}/{(total_playstats//batch_size) + 1}..."
            )
            execute_batch(cursor, playstats_insert_query, values, page_size=1000)

        # Refresh materialized views
        print("Refreshing materialized views...")
        cursor.execute("REFRESH MATERIALIZED VIEW player_game_stats")

        # Commit transaction
        conn.commit()
        print("Database seeding completed successfully!")

    except Exception as e:
        conn.rollback()
        print(f"Error seeding database: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    # Example usage
    start_time = time.time()
    load_dotenv()
    print(os.getenv("DB_HOST"))

    # Change these parameters to match your PostgreSQL configuration
    seed_database(
        db_name=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )

    elapsed_time = time.time() - start_time
    print(f"Total time elapsed: {elapsed_time:.2f} seconds")
