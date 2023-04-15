import concurrent.futures
from typing import List

import requests
import threading

from requests import Session

from utils.big_query_api import get_top_freq_queries_list

thread_local = threading.local()

BASE_URL = 'http://localhost:8080//related?'


def get_session() -> Session:
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def send_request(query: str) -> None:
    session = get_session()
    session.get(f"{BASE_URL}query={query}")


def send_all_requests(queries: List[str]):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(send_request, queries)


if __name__ == '__main__':
    top_queries = get_top_freq_queries_list(100)
    send_all_requests(top_queries)

