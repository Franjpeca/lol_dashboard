"""
Utility functions for date handling in metrics scripts.
Ensures consistent timezone handling across all metrics.
"""
from datetime import datetime, timezone


def date_to_timestamp_ms(date_str: str, end_of_day: bool = False) -> int:
    """
    Convert a date string (YYYY-MM-DD) to a UTC timestamp in milliseconds.
    
    Args:
        date_str: Date in format "YYYY-MM-DD"
        end_of_day: If True, returns timestamp for 23:59:59.999 of that day
                   If False, returns timestamp for 00:00:00 of that day
    
    Returns:
        Timestamp in milliseconds (UTC)
    
    Example:
        >>> date_to_timestamp_ms("2026-01-08", end_of_day=False)
        1736294400000  # 2026-01-08 00:00:00 UTC
        >>> date_to_timestamp_ms("2026-01-08", end_of_day=True)
        1736380799999  # 2026-01-08 23:59:59.999 UTC
    """
    # Parse date string as UTC (not local timezone)
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    
    if end_of_day:
        # Set to end of day: 23:59:59.999
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
    else:
        # Already at 00:00:00
        pass
    
    # Make timezone-aware as UTC
    dt_utc = dt.replace(tzinfo=timezone.utc)
    
    # Convert to milliseconds
    return int(dt_utc.timestamp() * 1000)
