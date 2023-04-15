import argparse

from utils.big_query_api import reduce_queries_by_session
from utils.constants import VALID_DB_TABLE_NAMES, LIST_LENGTH
from utils.functions import convert_data_to_dict, sort_related_queries_by_freq, construct_entities_list, \
    insert_all_entities_to_datastore

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="fill datastore",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-dn", "--dbname", help="db name to perform querying")
    args = parser.parse_args()
    db_name = args.dbname
    if db_name not in VALID_DB_TABLE_NAMES:
        raise KeyError("You should provide valid DB table name!")
    print(f'db_name = {db_name}')
    print('Querying BigQuery...')
    rows = reduce_queries_by_session(db_name)
    print('Converting data to dictionary...')
    queries_related = convert_data_to_dict(rows)
    print('Sorting queries by frequency...')
    sorted_queries = sort_related_queries_by_freq(queries_related)
    print('Prepare entities for datastore...')
    entities = construct_entities_list(sorted_queries, LIST_LENGTH)
    print('Inserting entities in datastore...')
    insert_all_entities_to_datastore(entities)
    print('Done.')


