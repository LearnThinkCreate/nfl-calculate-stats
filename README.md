# NFL-Calculate-Stats

A comprehensive NFL statistics processing pipeline for analyzing play-by-play data.

## Overview

This repository provides a powerful, flexible framework for calculating NFL statistics from play-by-play data. The core function `calculate_stats()` serves as the primary entry point, orchestrating a complete data pipeline that loads, processes, and analyzes NFL play-by-play and play statistics data.

## Main Functionality: calculate_stats

The `calculate_stats()` function is the heart of this repository. It allows users to:

- Calculate comprehensive player or team statistics for specified NFL seasons
- Aggregate statistics at either season or week level
- Filter for regular season, postseason, or all games
- Process multiple seasons simultaneously

```python
def calculate_stats(
    seasons=most_recent_season(),
    summary_level="season",
    stat_type="player",
    season_type="REG"
):
    """Calculate NFL statistics based on specified parameters"""
    # Function implementation...
```

### Example Usage

```python
# Get player-level season stats for 2023
stats_2023 = calculate_stats(2023, "season", "player", "REG")

# Get week-by-week team stats for multiple seasons
weekly_team_stats = calculate_stats(
    seasons=[2021, 2022, 2023],
    summary_level="week",
    stat_type="team"
)
```

## Underlying Components

The statistics pipeline consists of several key components that work together:

### 1. Play-by-Play Data Processing

The `clean_pbp_data()` function in `clean_pbp.py` prepares raw NFL play-by-play data for analysis:

- Filters to essential columns via `filter_pbp_columns()`
- Normalizes team abbreviations for consistency
- Adds derived fields for enhanced analysis
- Handles missing values appropriately
- Validates data integrity

This cleaned data serves as the foundation for all statistical calculations.

### 2. Play Statistics Processing

The `process_playstats()` function in `playstats.py` enriches play-by-play data with detailed statistical flags:

- Loads and filters play statistics for specific game IDs
- Computes aggregations at player, team, and game levels
- Creates flag indicators for various play outcomes (completions, touchdowns, fumbles, etc.)
- Calculates yardage metrics (passing yards, rushing yards, air yards, etc.)
- Joins with team/opponent information

The result is a fully enriched dataset that identifies specific player actions on each play.

### 3. Offensive Play Statistics Extraction

Functions in `pbp_off_stats.py` extract specialized offensive statistics from the play-by-play data:

- `get_passing_stats()`: Calculates QB passing metrics (EPA, CPOE, success rate)
- `get_rushing_stats()`: Extracts rushing statistics
- `get_receiving_stats()`: Computes receiver statistics
- `get_dropback_stats()`: Analyzes quarterback dropback performance
- `get_scramble_stats()`: Evaluates quarterback scramble effectiveness

These functions perform targeted extraction and aggregation of specific play types.

### 4. Stats Aggregation and Processing

The `process_stats()` function in `process_stats.py` brings everything together:

- Aggregates all play statistics based on specified grouping variables
- Calculates advanced metrics like PACR, RACR, WOPR, and target share
- Handles team-level and player-level statistics differently
- Computes derived statistics (yards per attempt, completion percentage, etc.)
- Merges the specialized offensive statistics with the aggregated play stats
- Orders columns in a logical, consistent manner

This final step produces a comprehensive statistical table ready for analysis.

## Data Loading and Caching

One of the most powerful features of this repository is its sophisticated data loading and caching system:

### Local Data Caching

The `utils_data.py` module implements an efficient caching mechanism:

- Play-by-play data and play statistics are stored locally in Parquet format
- First request for a season's data downloads it; subsequent requests use the cached file
- The `update_season` parameter allows refreshing the current season with new games
- Data is stored in a structured directory hierarchy (`data/pbp/` and `data/playstats/`)

This approach minimizes API calls and dramatically improves performance for repeated analyses.

### R Integration for Play Statistics

One particularly innovative aspect is the integration with R to handle play statistics data:

- NFL play statistics are only available in RDS format (R's native serialization format)
- The `load_playstats_cli()` function executes an R script via subprocess
- `11_load_playstats_cli.R` and `10_load_playstats.R` handle downloading and converting data
- RDS files are transformed into Parquet format for efficient access from Python

This creative approach bridges the gap between R and Python ecosystems, making previously inaccessible data available.

### Season Detection

The `utils_date.py` module provides intelligent season detection:

- `most_recent_season()` automatically determines the current NFL season based on date
- Uses Labor Day (computed by `compute_labor_day()`) as a reference point
- Handles the NFL's unique calendar (season spans calendar years)
- Supports different logic for roster vs. game data

## Database Integration

For users who need persistent storage, the repository includes database integration:

- `seed_database()` in `seed.py` populates a PostgreSQL database with NFL data
- Efficiently loads teams, players, games, snap counts, and play data
- Uses batch operations for performance
- Implements proper transaction handling and error recovery
- Leverages both SQLAlchemy and direct psycopg2 connections for optimal performance

## Dimensional Data

The `clean_dims.py` module provides access to various dimensional data:

- `get_teams()`: Retrieves and cleans current NFL team information
- `get_players()`: Collects comprehensive player data
- `get_games()`: Obtains game schedules and metadata
- `get_snap_counts()`: Retrieves offensive snap count data

These functions provide crucial context for the statistical calculations.

## Conclusion

This NFL statistics processing pipeline provides a powerful, flexible framework for analyzing football data. By combining efficient data loading, sophisticated processing, and comprehensive statistical calculations, it enables deep insights into NFL player and team performance.
