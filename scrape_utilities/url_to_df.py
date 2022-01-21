
import pandas as pd
from functools import partial
from multiprocessing import Pool
from time import time, sleep
from math import ceil

from cleaning.file_management import pattern_merge


from scrape_utilities.url_request import RegularRequest

def URL_TO_DF(url, extractor_function):
    try:
        html, status = RegularRequest(url)
        df = extractor_function(html)
        df['url'], df['status'] = url, status
    except Exception as e:
        df=pd.DataFrame()
        df['url'], df['status'] = url, f'error: {e}'
        print(e)
    return df