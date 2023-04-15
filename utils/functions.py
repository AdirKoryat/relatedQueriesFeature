
from typing import Dict, List
import re

from google.cloud.bigquery.table import RowIterator

from db.db_operations import store_entity, get_value_by_key


def convert_data_to_dict(rows: RowIterator) -> Dict[str, Dict[str, int]]:
    d = {}
    for row in rows:
        query, related_query_seq = row['query'], row['related_queries']
        if not is_valid_query(query):
            continue
        related_query_list = related_query_seq.split("&&")
        for related_query in related_query_list:
            if related_query == query or not is_valid_query(related_query):
                continue
            if query in d:
                if related_query in d[query]:
                    d[query][related_query] += 1
                else:
                    d[query][related_query] = 1
            else:
                d[query] = {related_query: 1}
            if related_query in d:
                if query in d[related_query]:
                    d[related_query][query] += 1
                else:
                    d[related_query][query] = 1
            else:
                d[related_query] = {query: 1}
    return d


def is_valid_query(query: str) -> bool:
    return bool(re.match("^[A-Za-z0-9][A-Za-z0-9 _-]+$", query))


def sort_related_queries_by_freq(related_queries: Dict[str, Dict[str, int]]) -> Dict[str, List[str]]:
    sorted_queries = {}
    for item in related_queries:
        sorted_keys = sorted(related_queries[item], key=related_queries[item].get, reverse=True)
        sorted_queries[item] = list(sorted_keys)
    return sorted_queries


def construct_entities_list(queries: Dict[str, List[str]], list_length: int) -> Dict[str, Dict[str, List[str]]]:
    entities = {}
    for key in queries.keys():
        first_letter = key[0]
        if first_letter in entities:
            entities[first_letter].update({key: queries[key][:list_length]})
        else:
            entities[first_letter] = {key: queries[key][:list_length]}
    return entities


def insert_all_entities_to_datastore(entity_dict: Dict[str, Dict[str, List[str]]]) -> None:
    for k in entity_dict:
        store_entity(k, entity_dict[k])


def serve_request(query_name: str) -> str:
    queries = get_value_by_key(query_name[0])
    if len(queries) > 0:
        try:
            res = queries[query_name]
        except KeyError:
            res = []
        if len(res) <= 0:
            output = f'No results found for {query_name}.'
        else:
            output = res
    else:
        output = f'No results found for {query_name}.'
    return output







