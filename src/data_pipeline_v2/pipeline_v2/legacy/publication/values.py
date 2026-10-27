"""Publication boundary value encoding without opening a database."""
from datetime import datetime
from ..contracts import utc


def sql_time(value):
    return datetime.fromisoformat(utc(value).replace('Z','+00:00')).replace(tzinfo=None) if value else None
