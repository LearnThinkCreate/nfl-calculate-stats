import pandas as pd
import numpy as np # Import numpy for np.nan check if needed
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine # Keep for teams insert for now
import time
from utils_data import load_pbp
from clean_pbp import clean_pbp_data
from playstats import process_playstats
from clean_dims import get_games, get_players, get_teams, get_snap_counts
from dotenv import load_dotenv
import os

load_dotenv()

# --- Main Seeding Function ---
def seed_database(db_name="nfl_stats", user="xxxx", password="xxxx", host="localhost", port="5432"):
    """
    Seed the NFL stats database with teams, players, games, plays, playstats, and snap counts.
    """
    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(conn_string) # Keep engine for initial teams insert if preferred
    conn = None # Initialize conn to None
    
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = False # Ensure transaction control
        cursor = conn.cursor()

        print("Fetching data...")
        seasons = range(1999, 2025)
        
        
        teams_df = get_teams()
        players_df = get_players()
        games_df = get_games(seasons)
        snap_counts_df = get_snap_counts(seasons)
        
        print("Loading and cleaning play-by-play data...")
        pbp_df = clean_pbp_data(load_pbp(seasons))
        
        print("Processing playstats...")
        playstats_df = process_playstats(pbp_df, seasons, "week")
        # Ensure 'special' column exists and fillna (as requested)
        if 'special' in playstats_df.columns:
            playstats_df['special'] = playstats_df['special'].fillna(0)
        else:
             print("Warning: 'special' column not found in playstats_df after processing.")
             # Handle as needed, maybe add it: playstats_df['special'] = 0

        print("Data fetching and initial processing complete.")

        # Start transaction
        print("Beginning database import...")

        print("Importing teams...")
        teams_df.replace({pd.NA: None, np.nan: None}).to_sql(
             'teams', 
             engine, 
             if_exists='append', # Consider 'replace' if you want a full refresh, or handle conflicts
             index=False,
             method=None # Let sqlalchemy handle it, often slower but simpler for small tables
        )

        player_insert_cols = [ # Match 03_players.sql
            'gsis_id', 'status', 'display_name', 'first_name', 'last_name', 'esb_id', 'birth_date', 
            'college_name', 'position', 'jersey_number', 'height', 'weight', 'team_abbr', 
            'current_team_id', 'entry_year', 'rookie_year', 'draft_club', 'college_conference', 
            'status_short_description', 'gsis_it_id', 'short_name', 'headshot', 'draft_number', 'draftround'
        ]
        game_insert_cols = [ # Match 04_games.sql
            'game_id', 'season', 'game_type', 'week', 'gameday', 'weekday', 'gametime', 
            'away_team', 'away_score', 'home_team', 'home_score', 'location', 'result', 
            'total', 'overtime', 'gsis', 'nfl_detail_id', 'pfr', 'pff', 'espn', 'ftn', 
            'away_rest', 'home_rest', 'away_moneyline', 'home_moneyline', 'spread_line', 
            'away_spread_odds', 'home_spread_odds', 'total_line', 'under_odds', 'over_odds', 
            'div_game', 'roof', 'surface', 'temp', 'wind', 'away_qb_id', 'home_qb_id', 
            'away_qb_name', 'home_qb_name', 'away_coach', 'home_coach', 'stadium_id', 'stadium'
        ]
        snap_counts_insert_cols = [ # Match 07_snap_counts.sql
            'gsis_id', 'game_id', 'season', 'week', 'offense_snaps', 'offense_pct'
        ]
        
        # Assumes pbp_df and playstats_df columns match 05_pbp.sql and 06_playstats.sql exactly
        # If not, define explicit lists for them too.
        plays_insert_cols = None # Use all columns from pbp_df
        playstats_insert_cols = None # Use all columns from playstats_df

        # --- Insert Players (Using Specific Columns) ---
        player_pk = ['gsis_id']
        player_update_cols = ['status', 'team_abbr'] 
        execute_batch_insert(cursor, players_df, 'players', player_pk, 
                             True, player_update_cols, 
                             insert_columns=player_insert_cols)

        # --- Insert Games (Using Specific Columns) ---
        game_pk = ['game_id']
        game_update_cols = ['away_score', 'home_score'] 
        execute_batch_insert(cursor, games_df, 'games', game_pk, 
                             True, game_update_cols, 
                             insert_columns=game_insert_cols)

        # --- Insert Snap Counts (Using Specific Columns) ---
        snap_counts_pk = ['gsis_id', 'game_id']
        snap_counts_update_cols = ['offense_snaps', 'offense_pct'] 
        execute_batch_insert(cursor, snap_counts_df, 'snap_counts', snap_counts_pk, 
                             True, snap_counts_update_cols, 
                             insert_columns=snap_counts_insert_cols)

        # --- Insert Plays (Assuming all df columns are needed) ---
        plays_pk = ['season', 'play_id', 'game_id'] 
        execute_batch_insert(cursor, pbp_df, 'plays', plays_pk, 
                             False, update_cols=[], 
                             insert_columns=plays_insert_cols,
                             page_size=10_000) 

        # --- Insert Playstats (Assuming all df columns are needed) ---
        playstats_pk = ['season', 'play_id', 'game_id', 'player_id', 'stat_id'] 
        execute_batch_insert(cursor, playstats_df, 'playstats', playstats_pk, 
                             False, update_cols=[], 
                             insert_columns=playstats_insert_cols,
                             page_size=10_000) 

        # Refresh materialized views
        print("Refreshing materialized views...")
        cursor.execute("REFRESH MATERIALIZED VIEW player_game_stats")
        
        conn.commit()
        print("Database seeding completed successfully!")


    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error seeding database: {error}")
        if conn:
            conn.rollback()
            print("Transaction rolled back.")
        raise # Re-raise the exception after rollback

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
        if engine:
             engine.dispose() # Dispose sqlalchemy engine if used


def generate_on_conflict_update_sql(df_columns, pk_columns):
    """Generates the SET clause for ON CONFLICT DO UPDATE."""
    update_cols = [col for col in df_columns if col not in pk_columns]
    if not update_cols:
        return "updated_at = NOW()" # Default if only PKs

    set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
    set_clause += ", updated_at = NOW()"
    return set_clause


def execute_batch_insert(cursor, df, table_name, pk_columns, 
                         conflict_action=False, update_cols=None, 
                         insert_columns=None, # <-- Add optional parameter
                         page_size=1000):
    """
    Performs batch insert using psycopg2.extras.execute_batch, handling NumPy types
    and allowing specific column insertion.

    Args:
        cursor: Database cursor.
        df: Pandas DataFrame to insert.
        table_name: Name of the target database table.
        pk_columns: List of primary key column names for conflict target.
        conflict_action: false 'DO NOTHING', true 'DO UPDATE'.
        update_cols: List of columns to update on conflict, or 'all' to update all non-PK columns.
                     Only used if conflict_action is 'DO UPDATE'.
        insert_columns: Optional list of column names from the DataFrame to insert. 
                        If None, all columns from the DataFrame are used.
        page_size: Number of rows per batch insert.
    """
    if df.empty:
        print(f"Skipping insert for empty DataFrame into {table_name}.")
        return

    # Determine which columns to use for the INSERT statement
    cols_to_insert = insert_columns if insert_columns else list(df.columns)
    
    # Check if specified insert_columns actually exist in the DataFrame
    missing_cols = [col for col in cols_to_insert if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns specified in insert_columns not found in DataFrame for table {table_name}: {missing_cols}")

    print(f"Preparing insert for {table_name} ({len(df)} rows)")

    # Ensure consistent column quoting for safety
    quoted_cols_to_insert = [f'"{col}"' for col in cols_to_insert]
    cols_str = ", ".join(quoted_cols_to_insert)
    placeholders = ", ".join(["%s"] * len(cols_to_insert))
    
    # PK columns must exist in the cols_to_insert list if they are needed for ON CONFLICT
    missing_pk_in_insert = [pk for pk in pk_columns if pk not in cols_to_insert]
    if missing_pk_in_insert:
         raise ValueError(f"Primary key columns {missing_pk_in_insert} must be included in insert_columns for table {table_name} when using ON CONFLICT.")
    
    pk_cols_str = ", ".join([f'"{col}"' for col in pk_columns])

    sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

    # --- ON CONFLICT Logic (using cols_to_insert for context if needed) ---
    if conflict_action:
        if update_cols == 'all':
            # Generate update clause based on the columns being inserted, excluding PKs
            set_clause = generate_on_conflict_update_sql(cols_to_insert, pk_columns)
        elif isinstance(update_cols, list):
             # Ensure update_cols are quoted and add updated_at
             set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
             set_clause += ", updated_at = NOW()"
        else: # Default to updating only updated_at
            set_clause = "updated_at = NOW()"
            
        sql += f" ON CONFLICT ({pk_cols_str}) DO UPDATE SET {set_clause}"   
    else: 
        pass # Simple insert

    # Prepare data: Select only the required columns, replace NA/NaN, convert types
    print(f"Selecting and converting data types for {table_name}...")
    start_conv = time.time()

    # Select only the necessary columns *before* iterating
    df_subset = df[cols_to_insert]

    data_tuples = []
    for row_tuple in df_subset.replace({pd.NA: None, np.nan: None}).itertuples(index=False, name=None):
        processed_row = []
        for item in row_tuple:
            if isinstance(item, np.integer):
                processed_row.append(int(item))
            elif isinstance(item, np.floating):
                processed_row.append(float(item))
            elif isinstance(item, np.bool_):
                processed_row.append(bool(item))
            # Handle pandas Timestamps explicitly if they cause issues
            elif isinstance(item, pd.Timestamp):
                 processed_row.append(item.to_pydatetime()) # Convert to python datetime
            else:
                processed_row.append(item)
        data_tuples.append(tuple(processed_row))

    conv_time = time.time() - start_conv
    print(f"Data selection and conversion for {table_name} took {conv_time:.2f}s")

    if not data_tuples:
        print(f"No data tuples generated for {table_name}, skipping execute_batch.")
        return

    try:
        print(f"Executing batch insert for {table_name}...")
        start_conv = time.time()
        execute_batch(cursor, sql, data_tuples, page_size=page_size)
        conv_time = time.time() - start_conv
        print(f"Successfully inserted/updated data for {table_name} in {conv_time:.2f}s.")
    except Exception as e:
        # Try to get a more informative error message with mogrify
        error_msg = f"Error during batch insert for {table_name}: {e}"
        try:
            example_sql = cursor.mogrify(sql, data_tuples[0]).decode('utf-8', errors='ignore') # Decode bytes
            error_msg += f"\nFailed SQL (sample): {example_sql}"
        except Exception as me:
             error_msg += f"\n(Could not generate sample SQL with mogrify: {me})"
        
        print(error_msg)
        raise # Re-raise the original exception to trigger rollback


if __name__ == "__main__":
    start_time = time.time()
    
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    db_port = os.getenv("DB_PORT")

    if not all([db_host, db_name, db_user, db_pass, db_port]):
         print("Error: Database environment variables (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT) are not fully set.")
    else:
        print(f"Attempting to connect to {db_name} on {db_host}:{db_port} as {db_user}")
        seed_database(
            db_name=db_name,
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port
        )

    elapsed_time = time.time() - start_time
    print(f"Total time elapsed: {elapsed_time:.2f} seconds")