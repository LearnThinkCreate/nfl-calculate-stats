import numpy as np


def clean_grp_vctr(grp_vctr):
    # remove player_id or team from grp_vctr
    return [col for col in grp_vctr if col not in ["player_id", "team"]]


def get_passing_stats(pbp, grp_vctr):
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
