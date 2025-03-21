# load_playstats_cli.R

library(optparse)

# Define command-line options
option_list <- list(
  make_option(c("-f", "--file_pattern"), type = "character", default = "data/playstats/playstats_%s.parquet",
              help = "File path pattern for saving Parquet files, must contain '%s' for the season [default %default]"),
  make_option(c("-s", "--seasons"), type = "character", default = NULL,
              help = "Comma-separated list of seasons (e.g., 2020,2021). Defaults to the most recent season if not provided.")
)

# Parse options
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# Source the core function
source("10_load_playstats.R")

# Handle seasons
if (is.null(opt$seasons)) {
  seasons <- nflreadr::most_recent_season()
} else {
  seasons <- as.numeric(strsplit(opt$seasons, ",")[[1]])
}

# Validate seasons
if (any(is.na(seasons)) || any(seasons < 1999) || any(seasons > nflreadr::most_recent_season())) {
  stop("Invalid season(s) provided. Seasons must be between 1999 and the most recent season.")
}

# Execute the function
load_playstats(seasons = seasons, file_pattern = opt$file_pattern)

cat("Data loaded and saved successfully.\n")