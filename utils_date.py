import datetime
from typing import Optional


def compute_labor_day(year: int) -> datetime.date:
    """
    Compute the date of Labor Day (first Monday in September) for a given year.

    Args:
        year: The year to compute Labor Day for

    Returns:
        datetime.date: The date of Labor Day for the given year
    """
    if not isinstance(year, int):
        raise TypeError("Year must be an integer")

    # Start with September 1st
    earliest = datetime.date(year, 9, 1)

    # Find the first Monday (weekday 0) in September
    days_until_monday = (7 - earliest.weekday()) % 7
    if days_until_monday == 0:  # If September 1st is already a Monday
        return earliest
    else:
        return earliest + datetime.timedelta(days=days_until_monday)


def most_recent_season(roster: bool = False) -> int:
    """
    Determine the most recent NFL season based on the current date.

    Args:
        roster: If True, use roster-specific logic for determining the season

    Returns:
        int: The year of the most recent NFL season
    """
    today = datetime.date.today()
    current_year = today.year
    current_month = today.month
    current_day = today.day

    # First Monday of September (Labor Day)
    labor_day = compute_labor_day(current_year)

    # Thursday following Labor Day is TNF season opener
    season_opener = labor_day + datetime.timedelta(days=3)

    if (
        (not roster and today >= season_opener)
        or (roster and current_month == 3 and current_day >= 15)
        or (roster and current_month > 3)
    ):
        return current_year

    return current_year - 1
