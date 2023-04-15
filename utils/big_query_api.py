from typing import List

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator

# Construct a BigQuery client object.
client = bigquery.Client()


def reduce_queries_by_session(db_name: str) -> RowIterator:
    query = f"""
    SELECT query, STRING_AGG(related_queries, '&&') as related_queries 
    FROM (SELECT MIN(query) as query,  STRING_AGG(query, '&&') as related_queries 
          FROM `simpledatabase-382009.big_query_test.{db_name}`
          GROUP BY session)
    WHERE related_queries LIKE '%&&%'
    GROUP BY query
    """

    query_job = client.query(query)  # API request
    rows = query_job.result()  # Waits for query to finish
    return rows


def get_top_freq_queries_list(max_records: int) -> List[str]:
    query = f"""
    SELECT COUNT(session) as num_sessions,  query FROM `simpledatabase-382009.big_query_test.USER_DATA`
    GROUP BY query
    ORDER BY num_sessions DESC
    LIMIT {max_records}
    """
    query_job = client.query(query)  # API request
    rows = query_job.result()  # Waits for query to finish
    res = [row['query'] for row in rows]
    return res
