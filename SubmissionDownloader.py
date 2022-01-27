""" Class to download submission data from Pushshift and Reddit's API."""

from typing import Optional, List, Tuple
import warnings
import time
from requests_cache import CachedSession
import arrow
import praw


class SubmissionDownloader:
    PUSHSHIFT_URL = 'https://api.pushshift.io/reddit/search/submission/'

    def __init__(self, pushshift):
        self.pushshift_session = CachedSession(pushshift, backend='sqlite')

    def _pushshift_api(self, subreddit: str,
                       start: arrow.arrow.Arrow,
                       end: arrow.arrow.Arrow) -> Tuple:
        payload = {
            'subreddit': subreddit,
            'size': 500,
            'before': round(end.timestamp()),
            'after': round(start.timestamp())
        }
        r = self.pushshift_session.get(self.PUSHSHIFT_URL, params=payload)
        return (r.json()['data'], r.from_cache)

    def get_subreddit_submissions(self,
                                  subreddit: str,
                                  start: arrow.arrow.Arrow,
                                  end: arrow.arrow.Arrow,
                                  hour_window: Optional[int] = 4,
                                  wait_time: Optional[float] = 0.1) -> List:
        """ Uses the Pushshift API to obtain the submissions to a subreddit
            between 'start' and 'end'."""
        all_data = []
        while start < end:
            end_window = start.shift(hours=hour_window)
            data, cached = self._pushshift_api(subreddit, start, end_window)
            all_data += data
            if len(data) >= 100:
                warnings.warn("Maximum number of items returned. Consider " +
                              "reducing hour_window.")

            if len(data):
                start = arrow.get(data[-1]['created_utc'])
            else:
                start = end_window

            if not cached:
                time.sleep(wait_time)

        return all_data

    def update_subreddit_submissions(self,
                                     submissions: List,
                                     attributes: List[str]) -> List:
        """ Uses the official Reddit API to update submissions
            with current data."""
        reddit = praw.Reddit()

        fullnames = [f"t3_{s['id']}" for s in submissions]
        reddit_data = reddit.info(fullnames=fullnames)
        for (index, item) in enumerate(reddit_data):
            for attr in attributes:
                submissions[index][attr] = getattr(item, attr)

        return submissions
