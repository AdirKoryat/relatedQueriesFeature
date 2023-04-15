from unittest import TestCase

from utils.functions import serve_request


class Test(TestCase):
    def test_popular_query_return_expected_list(self) -> None:
        expected_output = ['black dress', 'mickey', 'jordan']
        query = 'disney'
        output = serve_request(query)
        self.assertEqual(output, expected_output)

    def test_unpopular_query_return_expected_list(self) -> None:
        expected_output = ['air force 1']
        query = 'pokey'
        output = serve_request(query)
        self.assertEqual(output, expected_output)

    def test_non_query_exists_return_empty_list(self) -> None:
        query = 'nononono'
        expected_output = f'No results found for {query}.'
        output = serve_request(query)
        self.assertEqual(output, expected_output)
