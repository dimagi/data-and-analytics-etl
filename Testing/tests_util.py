import boto3
import importlib
import unittest
import unittest.mock

from unittest.mock import MagicMock
from Testing.util import (
    MockResponse,
    generate_get_boto3_client_mock_function,
    run_test_cases
)

get_boto3_client_mock = generate_get_boto3_client_mock_function(
    {
        'ssm': {
            'parameters': {
                'test_pass': 'test',
                'test_domain_pass-api-key': 'test_1',
                'test_domain_pass-test_specifier-api-key': 'test_2'
            },
            'objects': {}
        }
    }
)

boto3.client = MagicMock(side_effect=get_boto3_client_mock)

import util
importlib.reload(util)


def raise_api_error(data_type):
    raise util.APIError(f"Request failed for data type {data_type}!", 400)


class TestUtil(unittest.TestCase):
    def test_process_response(self):
        print('*** Running test_process_response ***')
        test_data = [
            {
                'name': 'is_boto_200',
                'parameters': {
                    'response': {
                        'ResponseMetadata': {
                            'HTTPStatusCode': 200
                        }
                    },
                    'is_boto': True
                },
                'return_value': None,
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'is_boto_400',
                'parameters': {
                    'response': {
                        'ResponseMetadata': {
                            'HTTPStatusCode': 400
                        }
                    },
                    'is_boto': True
                },
                'return_value': None,
                'expect_exception': True,
                'exception': Exception("Boto3 request failed! Code: 400.")
            },
            {
                'name': 'is_not_boto_ok',
                'parameters': {
                    'response': MockResponse(),
                    'is_boto': False
                },
                'return_value': {},
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'is_not_boto_not_ok',
                'parameters': {
                    'response': MockResponse(
                        status_code=400, ok=False, reason='Test'),
                    'is_boto': False
                },
                'return_value': None,
                'expect_exception': True,
                'exception': util.APIError(
                    "Request failed! Code: 400. Reason: Test", 400)
            }
        ]

        def test_function(self, test_case):
            response = util.process_response(
                test_case['parameters']['response'],
                test_case['parameters']['is_boto']
            )
            if test_case['return_value'] is None:
                self.assertIsNone(response)
            else:
                self.assertDictEqual(
                    test_case['return_value'], response)

        run_test_cases(self, test_data, test_function)

    def test_get_value_from_parameter_store(self):
        print('*** Running test_process_response ***')
        test_data = [
            {
                'name': 'get_value_success',
                'parameters': {
                    'param_name': 'test_pass'
                },
                'return_value': 'test',
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'get_value_fail',
                'parameters': {
                    'param_name': 'test_fail'
                },
                'return_value': None,
                'expect_exception': True,
                'exception': Exception("Boto3 request failed! Code: 400.")
            }
        ]

        def test_function(self, test_case):
            param = util.get_value_from_parameter_store(
                test_case['parameters']['param_name']
            )
            if test_case['return_value'] is None:
                self.assertIsNone(param)
            else:
                self.assertEqual(test_case['return_value'], param)

        run_test_cases(self, test_data, test_function)

    def test_get_api_token(self):
        print('*** Running test_process_response ***')
        test_data = [
            {
                'name': 'get_api_token_no_specifier_success',
                'parameters': {
                    'domain': 'test_domain_pass',
                    'specifier': None
                },
                'return_value': 'test_1',
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'get_api_token_no_specifier_fail',
                'parameters': {
                    'domain': 'test_domain_fail',
                    'specifier': None
                },
                'return_value': None,
                'expect_exception': True,
                'exception': Exception("Boto3 request failed! Code: 400.")
            },
            {
                'name': 'get_api_token_with_specifier_success',
                'parameters': {
                    'domain': 'test_domain_pass',
                    'specifier': 'test_specifier'
                },
                'return_value': 'test_2',
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'get_api_token_with_specifier_fail',
                'parameters': {
                    'domain': 'test_domain_fail',
                    'specifier': 'test_specifier'
                },
                'return_value': None,
                'expect_exception': True,
                'exception': Exception("Boto3 request failed! Code: 400.")
            }
        ]

        def test_function(self, test_case):
            api_token = util.get_api_token(
                test_case['parameters']['domain'],
                test_case['parameters']['specifier']
            )
            if test_case['return_value'] is None:
                self.assertIsNone(api_token)
            else:
                self.assertEqual(test_case['return_value'], api_token)

        run_test_cases(self, test_data, test_function)

    def test_put_value_parameter_store(self):
        print('*** Running test_process_response ***')
        test_data = [
            {
                'name': 'put_value_no_overwrite_success',
                'parameters': {
                    'param_name': 'test_put',
                    'param_value': 'return_value_1',
                    'overwrite': False
                },
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'put_value_no_overwrite_fail',
                'parameters': {
                    'param_name': 'test_put',
                    'param_value': 'return_value_2',
                    'overwrite': False
                },
                'expect_exception': True,
                'exception': Exception("Boto3 request failed! Code: 400.")
            },
            {
                'name': 'put_value_overwrite_success',
                'parameters': {
                    'param_name': 'test_put',
                    'param_value': 'return_value_3',
                    'overwrite': True
                },
                'expect_exception': False,
                'exception': None
            },
        ]

        def test_function(self, test_case):
            util.put_value_parameter_store(
                test_case['parameters']['param_name'],
                test_case['parameters']['param_value'],
                test_case['parameters']['overwrite']
            )
            param = util.get_value_from_parameter_store(
                test_case['parameters']['param_name']
            )
            if test_case['parameters']['param_value'] is None:
                self.assertIsNone(param)
            else:
                self.assertEqual(
                    test_case['parameters']['param_value'], param)

        run_test_cases(self, test_data, test_function)


if __name__ == '__main__':
    unittest.main()
