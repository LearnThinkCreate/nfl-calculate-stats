import datetime


def compute_labor_day(year: int) -> datetime.date:
    """
    Compute the date of Labor Day (first Monday in September) for a given year.
    
    Labor Day is observed on the first Monday in September in the United States.
    This function calculates that date for any given year using Python's datetime
    module. It starts with September 1st and calculates how many days to advance
    to reach the first Monday.
    
    Parameters
    ----------
    year : int
        The year for which to compute Labor Day
        
    Returns
    -------
    datetime.date
        The date object representing Labor Day (first Monday in September) 
        for the specified year
        
    Raises
    ------
    TypeError
        If the year parameter is not an integer
        
    Examples
    --------
    >>> compute_labor_day(2023)
    datetime.date(2023, 9, 4)
    
    >>> compute_labor_day(2024)
    datetime.date(2024, 9, 2)
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
    
    The NFL season typically runs from September through February of the following year.
    This function determines the most recent season based on the current date and NFL
    schedule patterns. It uses Labor Day (the first Monday in September) as a reference
    point, as the NFL regular season typically begins the Thursday after Labor Day.
    
    This function handles two different contexts:
    1. For general NFL data (roster=False): The season is considered to have started 
       if today's date is on or after the Thursday following Labor Day.
    2. For roster data (roster=True): The season is considered to have started 
       if today's date is on or after March 15th, which approximates the start of 
       NFL free agency.
    
    Parameters
    ----------
    roster : bool, default=False
        Whether to use roster-specific logic for determining the season.
        If True, uses free agency timing (mid-March) as the cutoff.
        If False, uses regular season timing (Thursday after Labor Day) as the cutoff.
        
    Returns
    -------
    int
        The year representing the most recent NFL season
        
    Examples
    --------
    # If today is August 1, 2024 (before season start)
    >>> most_recent_season()
    2023
    
    # If today is October 1, 2024 (after season start)
    >>> most_recent_season()
    2024
    
    # If today is April 1, 2024 (after free agency, before season start)
    >>> most_recent_season(roster=True)
    2024
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
