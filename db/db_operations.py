import json
from functools import lru_cache
from typing import Dict, List

from google.cloud import datastore

from utils.constants import KIND, VALUE_ATTRIBUTE

datastore_client = datastore.Client()


def store_entity(key: str, value: Dict[str, List[str]]) -> None:
    complete_key = datastore_client.key(KIND, key)
    entity = datastore.Entity(key=complete_key, exclude_from_indexes=(VALUE_ATTRIBUTE,))

    entity.update(
        {
            VALUE_ATTRIBUTE: json.dumps(value)
        }
    )
    datastore_client.put(entity)


@lru_cache
def get_value_by_key(key: str) -> Dict[str, List[str]]:
    complete_key = datastore_client.key(KIND, key)
    entity = datastore_client.get(complete_key)
    value = {}
    if entity:
        value = eval(entity[VALUE_ATTRIBUTE])
    return value
