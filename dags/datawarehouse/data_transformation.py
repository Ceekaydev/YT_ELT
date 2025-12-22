from datetime import timedelta, datetime
import re
from datawarehouse.data_utils import safe_sentiment


import re
from datetime import timedelta

def parse_duration(duration_str):
    """
    Parses an ISO 8601 duration string into a timedelta object.
    Supports:
      - PT8M54S
      - PT1H2M10S
      - P1DT2S
      - P2DT3H4M5S
      - HH:MM:SS
    """

    try:
        if not duration_str:
            raise ValueError("Empty duration")

        # ISO 8601 duration (with optional days and time)
        iso_match = re.fullmatch(
            r"P"
            r"(?:(\d+)D)?"
            r"(?:T"
            r"(?:(\d+)H)?"
            r"(?:(\d+)M)?"
            r"(?:(\d+)S)?"
            r")?",
            duration_str
        )

        if iso_match:
            days = int(iso_match.group(1) or 0)
            hours = int(iso_match.group(2) or 0)
            minutes = int(iso_match.group(3) or 0)
            seconds = int(iso_match.group(4) or 0)

            return timedelta(
                days=days,
                hours=hours,
                minutes=minutes,
                seconds=seconds
            )

        # HH:MM:SS fallback
        if re.fullmatch(r"\d{1,2}:\d{2}:\d{2}", duration_str):
            hours, minutes, seconds = map(int, duration_str.split(":"))
            return timedelta(
                hours=hours,
                minutes=minutes,
                seconds=seconds
            )

        raise ValueError(f"Invalid duration format: {duration_str}")

    except Exception as e:
        raise ValueError(f"Error parsing duration '{duration_str}': {e}")

    
# def parse_published_at(published_at: str) -> datetime:
#     """
#     Converts ISO 8601 YouTube publishedAt string to Python datetime.
#     Example: '2013-01-27T01:52:40Z'
#     """
#     try:
#         return datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
#     except Exception as e:
#         raise ValueError(f"Error parsing publishedAt '{published_at}': {e}")
    

def transform_data(row):
    """Transforms the input row by parsing the duration and categorizing video type.
     Args:
         row (dict): A dictionary representing a video's data.

     Returns:
         dict: The transformed row with updated 'duration' and new 'video_Type'.
     Raises:
         ValueError: If there is an error in parsing duration or transforming data."""
    try:
        # Parse Duration
        duration_td = parse_duration(row['duration'])

        row['duration'] = (datetime.min + duration_td).time().isoformat() # Convert to HH:MM:SS format

        row['video_type'] = "Short" if duration_td.total_seconds() < 60 else "Normal" # Categorize Video Type


        # # Parse published date
        # if isinstance(row.get("publishedAt"), str):
        #     row['publishedAt'] = parse_published_at(row['publishedAt'])

        # Sentiment Analysis for Video Title
        if not row.get("title_sentiment"): # Avoid re-computation if already present
            sentiment = safe_sentiment(row.get("title", ""))
            row['title_sentiment'] = sentiment['label'].capitalize()
            row['sentiment_score'] = sentiment['score']

        return row 
    
    except Exception as e:
        raise ValueError(f"Error transforming data for Video ID {row.get('video_ID', 'Unknown')}: {e}")