

import time
from stem.control import Controller
from stem import Signal
import requests
import random
from datetime import datetime
import collections
from fake_useragent import UserAgent
from multiprocessing import Pool
from cleaning.file_management import pattern_merge
import pandas as pd
from pandas.errors import EmptyDataError
from glob import glob
import os
import shutil
from pathlib import Path


def RegularRequest(url):
    '''backup request method if TOR is disabled'''
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US',
        'Connection': 'keep-alive',
        'Cookie': 'Cookie; Cookie',
        'DNT': '1',
        # 'Host': 'e2.kase.gov.lv',
        # 'Referer': 'https://www.cars.com/',
        # 'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Safari/605.1.15'
    }

    session = requests.session()
    html = session.get(url, headers=headers, timeout=30)
    status = html.status_code
    return html.text, status