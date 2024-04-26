import boto3
import importlib
import requests
import json
import unittest
import unittest.mock

from datetime import datetime
from unittest.mock import MagicMock
from testing.const import POST
from testing.requests_mock import mock_get, mock_request
from testing.util import (
    fake_json_file_load,
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
            'objects': {
                'commcare-snowflake-data-sync': [
                    {
                        'Key': 'test_domain/payload/test_specifier_1/2024/01/01/00/test_1.json',
                        'Body': {
                            'value': 'test_1.1'
                        }
                    },
                    {
                        'Key': 'test_domain/payload/test_specifier_1/2024/01/01/00/test_2.json',
                        'Body': {
                            'value': 'test_1.2'
                        }
                    },
                    {
                        'Key': 'test_domain/payload/test_specifier_2/2024/01/01/00/test_1.json',
                        'Body': {
                            'value': 'test_2.1'
                        }
                    },
                    {
                        'Key': 'test_domain/payload/test_specifier_2/2024/01/01/00/test_2.json',
                        'Body': {
                            'value': 'test_2.2'
                        }
                    }
                ]
            }
        }
    }
)

boto3.client = MagicMock(side_effect=get_boto3_client_mock)
requests.get = MagicMock(side_effect=mock_get)
requests.request = MagicMock(side_effect=mock_request)
json.load = MagicMock(side_effect=fake_json_file_load)

import CommCareAPIHandler
importlib.reload(CommCareAPIHandler)
from CommCareAPIHandler import CommCareAPIHandlerPush


class TestCommCareAPIHandlerPush(unittest.TestCase):
    def __init__(self, methodName):
        super().__init__(methodName)
        self.api = CommCareAPIHandlerPush(
            'test_domain',
            'test_domain-api-key',
            datetime.strptime(
                '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        )

    def test_commcareapihandlerpush_filepath(self):
        print('*** Running test_commcareapihandlerpush_filepath ***')
        test_data = [
            {
                'name': 'test_path',
                'parameters': {
                    'specifier': 'test_specifier'
                },
                'return_value': 'test_domain/payload/test_specifier/2024/01/01/00/',
                'expect_exception': False,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            file_path = self.api.filepath(
                test_case['parameters']['specifier']
            )
            if test_case['return_value'] is None:
                self.assertIsNone(file_path)
            else:
                self.assertEqual(
                    test_case['return_value'],
                    file_path
                )

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpush_get_request_content(self):
        print('*** Running test_commcareapihandlerpush_get_request_content ***')
        test_data = [
            {
                'name': 'test_specifier_1',
                'parameters': {
                    'specifier': 'test_specifier_1'
                },
                'return_value': [
                    {'value': 'test_1.1'},
                    {'value': 'test_1.2'}
                ],
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'test_specifier_2',
                'parameters': {
                    'specifier': 'test_specifier_2'
                },
                'return_value': [
                    {'value': 'test_2.1'},
                    {'value': 'test_2.2'}
                ],
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'test_specifier_missing',
                'parameters': {
                    'specifier': 'test_specifier_missing'
                },
                'return_value': None,
                'expect_exception': False,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            request_content = self.api._get_request_content(
                test_case['parameters']['specifier']
            )
            if test_case['return_value'] is None:
                self.assertIsNone(request_content)
            else:
                self.assertListEqual(
                    test_case['return_value'],
                    request_content
                )

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpush_push_data(self):
        print('*** Running test_commcareapihandlerpush_push_data ***')
        test_data = [
            {
                'name': 'test_specifier_1',
                'parameters': {
                    'data_type': {
                        'method': POST,
                        'version': 'test_version',
                        'name': 'test_name'
                    },
                    'specifier': 'test_specifier_1'
                },
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'test_specifier_2',
                'parameters': {
                    'data_type': {
                        'method': POST,
                        'version': 'test_version',
                        'name': 'test_name'
                    },
                    'specifier': 'test_specifier_2'
                },
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'test_specifier_missing',
                'parameters': {
                    'data_type': {
                        'method': POST,
                        'version': 'test_version',
                        'name': 'test_name'
                    },
                    'specifier': 'test_specifier_missing'
                },
                'expect_exception': False,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            self.api._push_data(
                test_case['parameters']['data_type'],
                test_case['parameters']['specifier']
            )

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpush_push_data_for_domain(self):
        print('*** Running test_commcareapihandlerpush_push_data_for_domain ***')
        test_data = [
            {
                'name': 'test_specifier_1',
                'parameters': {
                    'data_type': {
                        'method': POST,
                        'version': 'test_version',
                        'name': 'test_name'
                    },
                    'specifier': 'test_specifier_1'
                },
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'test_specifier_2',
                'parameters': {
                    'data_type': {
                        'method': 'PATCH',
                        'version': 'test_version',
                        'name': 'test_name'
                    },
                    'specifier': 'test_specifier_2'
                },
                'expect_exception': False,
                'exception': None
            },
            {
                'name': 'test_specifier_missing',
                'parameters': {
                    'data_type': {
                        'method': POST,
                        'version': 'test_version',
                        'name': 'test_name'
                    },
                    'specifier': 'test_specifier_missing'
                },
                'expect_exception': False,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            self.api.push_data_for_domain(
                test_case['parameters']['data_type'],
                test_case['parameters']['specifier']
            )

        run_test_cases(self, test_data, test_function)


if __name__ == '__main__':
    unittest.main()
