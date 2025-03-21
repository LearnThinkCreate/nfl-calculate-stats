# load_playstats.R

# Load required packages
require(nflreadr)
require(arrow)

# Helper function to clean team names
team_name_fn <- function(var) {
  stringr::str_replace_all(
    var,
    c(
      "JAC" = "JAX",
      "STL" = "LA",
      "SL" = "LA",
      "LAR" = "LA",
      "ARZ" = "ARI",
      "BLT" = "BAL",
      "CLV" = "CLE",
      "HST" = "HOU",
      "SD" = "LAC",
      "OAK" = "LV"
    )
  )
}

# Function to load and save playstats data
load_playstats <- function(seasons = nflreadr::most_recent_season(), file_pattern = "data/playstats/playstats_%s.parquet") {
  if (isTRUE(seasons)) seasons <- seq(1999, nflreadr::most_recent_season())
  
  # Validate inputs
  stopifnot(
    is.numeric(seasons),
    seasons >= 1999,
    seasons <= nflreadr::most_recent_season(),
    grepl("%s", file_pattern, fixed = TRUE)  # Ensure file_pattern has a placeholder
  )
  
  # Process each season
  for (s in seasons) {
    # Generate the full file path
    file_path <- sprintf(file_pattern, s)
    # Create the directory structure if it doesn't exist
    dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
    # Construct the URL for the season's data
    url <- paste0("https://github.com/nflverse/nflverse-pbp/releases/download/playstats/play_stats_", s, ".rds")
    # Load and clean the data
    data <- nflreadr::load_from_url(url, seasons = TRUE, nflverse = FALSE)
    data$team_abbr <- team_name_fn(data$team_abbr)
    # Save as Parquet
    arrow::write_parquet(data, file_path)
  }
}