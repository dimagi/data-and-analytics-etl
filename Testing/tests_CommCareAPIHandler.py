import boto3
import importlib
import unittest
import unittest.mock

from datetime import datetime
from unittest.mock import MagicMock
from testing.util import (
    generate_get_boto3_client_mock_function,
    run_test_cases
)

get_boto3_client_mock = generate_get_boto3_client_mock_function(
    {
        'ssm': {
            'parameters': {},
            'objects': {}
        },
        's3': {
            'parameters': {},
            'objects': {}
        }
    }
)

boto3.client = MagicMock(side_effect=get_boto3_client_mock)

import CommCareAPIHandler
importlib.reload(CommCareAPIHandler)
from CommCareAPIHandler import APIError, CommCareAPIHandler


def raise_api_error(data_type):
    raise APIError(f"Request failed for data type {data_type}!", 400)


class TestCommCareAPIHandler(unittest.TestCase):
    def test_commcareapihandler_init(self):
        print('*** Running test_commcareapihandler_init ***')
        test_data = [
            {
                'name': 'test_default_init',
                'api': CommCareAPIHandler(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'return_values': {
                    'domain': 'test_domain',
                    'api_token': 'test_domain-api-key',
                    'event_time': datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
                    'request_count': 0,
                    'request_limit': 100,
                    'APIErrorCount': 0,
                    'APIErrorMax': 3,
                    'custom_date_range_config': None,
                    'test_mode': False
                },
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'test_custom_init',
                'api': CommCareAPIHandler(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
                    request_limit=10,
                    custom_date_range_config='test',
                    test_mode=True
                ),
                'return_values': {
                    'domain': 'test_domain',
                    'api_token': 'test_domain-api-key',
                    'event_time': datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
                    'request_count': 0,
                    'request_limit': 10,
                    'APIErrorCount': 0,
                    'APIErrorMax': 3,
                    'custom_date_range_config': 'test',
                    'test_mode': True
                },
                'expect_exception': False,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            for key in test_case['return_values']:
                self.assertEqual(
                    getattr(test_case['api'], key),
                    test_case['return_values'][key]
                )

        run_test_cases(self, test_data, test_function)

    def test_api_base_url(self):
        print('*** Running test_api_base_url ***')
        test_data = [
            {
                'name': 'test_default_api_base_url',
                'api': CommCareAPIHandler(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'data_type': {
                        'version': 'test_version',
                        'name': 'test_name'
                    }
                },
                'return_value': 'https://www.commcarehq.org/a/test_domain/api/test_version/test_name/',
                'expect_exception': False,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            api_base_url = test_case['api'].api_base_url(
                test_case['parameters']['data_type']
            )
            if test_case['return_value'] is None:
                self.assertIsNone(api_base_url)
            else:
                self.assertEqual(
                    test_case['return_value'],
                    api_base_url
                )

        run_test_cases(self, test_data, test_function)

    def test_perform_method(self):
        print('*** Running test_perform_method ***')
        test_data = [
            {
                'name': 'test_under_api_error_threshold',
                'api': CommCareAPIHandler(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'method': raise_api_error
                },
                'repeat': 1,
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'test_above_api_error_threshold',
                'api': CommCareAPIHandler(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'method': raise_api_error
                },
                'repeat': 3,
                'expect_exception': True,
                'exception': APIError("Request failed for data type 2!", 400)
            }
        ]

        def test_function(self, test_case):
            for i in range(test_case['repeat']):
                test_case['api']._perform_method(
                    test_case['parameters']['method'],
                    i
                )

        run_test_cases(self, test_data, test_function)


if __name__ == '__main__':
    unittest.main()
