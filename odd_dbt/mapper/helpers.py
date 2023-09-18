from datetime import datetime
from typing import Optional

import pytz


def datetime_format(date: Optional[str]) -> Optional[datetime]:
    if date:
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(tz=pytz.utc)

    return None
