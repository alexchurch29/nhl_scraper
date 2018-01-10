import time
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

'''
Includes shared functions that will be called across multiple modules. 
'''


def get_url(url):
    """
    Get the url
    :param url: given url
    :return: raw html
    """
    response = requests.Session()
    retries = Retry(total=10, backoff_factor=.1)
    response.mount('http://', HTTPAdapter(max_retries=retries))

    response = response.get(url, timeout=5)
    response.raise_for_status()

    return response


def convert_to_seconds(minutes):
    """
    Return minutes remaining in time format to seconds elapsed
    :param minutes: time remaining
    :return: time elapsed in seconds
    """
    if minutes == '-16:0-':
        return '1200'      # Sometimes in the html at the end of the game the time is -16:0-

    import datetime
    x = time.strptime(minutes.strip(' '), '%M:%S')

    return datetime.timedelta(hours=x.tm_hour, minutes=x.tm_min, seconds=x.tm_sec).total_seconds()
