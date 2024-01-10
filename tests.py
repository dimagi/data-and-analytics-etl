
# from CommCareAPIHandler import CommCareAPIHandler, CommCareAPIHandlerPush

import boto3
import requests
import json
import unittest
import unittest.mock
from Testing.requests_mock import MockResponse, mock_get, mock_post
from Testing.boto3_mock import Boto3ClientMock
from datetime import datetime
from unittest.mock import MagicMock

def get_boto3_client_mock(Name):
    if Name == 'ssm':
        return Boto3ClientMock(
            parameters={
                'test_pass': 'test',
                'test_domain_pass-api-key': 'test_1',
                'test_domain_pass-test_specifier-api-key': 'test_2'
            }
        )
    elif Name == 's3':
        return Boto3ClientMock(
            objects={
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
        )

def fake_json_file_load(json_data):
    return json_data

boto3.client = MagicMock()
boto3.client.side_effect = get_boto3_client_mock
requests.get = MagicMock()
requests.get.side_effect = mock_get
requests.post = MagicMock()
requests.post.side_effect = mock_post
json.load = MagicMock()
json.load.side_effect = fake_json_file_load

import util
from CommCareAPIHandler import (
    CommCareAPIHandler,
    CommCareAPIHandlerPull,
    CommCareAPIHandlerPush
)


def raise_api_error(data_type):
    raise util.APIError(f"Request failed for data type {data_type}!", 400)


# TODO: Split tests into different files, one for util,
# one for CommCareAPIHandler, and one for each
# lambda function folder (when implemented)
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

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
                    response = util.process_response(
                        test_case['parameters']['response'],
                        test_case['parameters']['is_boto']
                    )
                    if test_case['return_value'] is None:
                        self.assertIsNone(response)
                    else:
                        self.assertDictEqual(
                            test_case['return_value'], response)
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))

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

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
                    param = util.get_value_from_parameter_store(
                        test_case['parameters']['param_name']
                    )
                    if test_case['return_value'] is None:
                        self.assertIsNone(param)
                    else:
                        self.assertEqual(test_case['return_value'], param)
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))

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

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
                    api_token = util.get_api_token(
                        test_case['parameters']['domain'],
                        test_case['parameters']['specifier']
                    )
                    if test_case['return_value'] is None:
                        self.assertIsNone(api_token)
                    else:
                        self.assertEqual(test_case['return_value'], api_token)
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))

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

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
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
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))


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

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
                    self.assertEqual(
                        test_case['api'].domain,
                        test_case['return_values']['domain']
                    )
                    self.assertEqual(
                        test_case['api'].api_token,
                        test_case['return_values']['api_token']
                    )
                    self.assertEqual(
                        test_case['api'].event_time,
                        test_case['return_values']['event_time']
                    )
                    self.assertEqual(
                        test_case['api'].request_count,
                        test_case['return_values']['request_count']
                    )
                    self.assertEqual(
                        test_case['api'].request_limit,
                        test_case['return_values']['request_limit']
                    )
                    self.assertEqual(
                        test_case['api'].APIErrorCount,
                        test_case['return_values']['APIErrorCount']
                    )
                    self.assertEqual(
                        test_case['api'].APIErrorMax,
                        test_case['return_values']['APIErrorMax']
                    )
                    self.assertEqual(
                        test_case['api'].custom_date_range_config,
                        test_case['return_values']['custom_date_range_config']
                    )
                    self.assertEqual(
                        test_case['api'].test_mode,
                        test_case['return_values']['test_mode']
                    )
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))

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

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
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
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))

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
                'exception': util.APIError("Request failed for data type 2!", 400)
            }
        ]

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
                    for i in range(test_case['repeat']):
                        test_case['api']._perform_method(
                            test_case['parameters']['method'],
                            i
                        )
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))


# TODO: Implement CommCareAPIHandlerPull tests
"""
class TestCommCareAPIHandlerPull(unittest.TestCase):
    def test_commcareapihandlerpull_filepath(self):
        self.assertEqual(True, False)

    def test_commcareapihandlerpull_get_last_job_success_time(self):
        self.assertEqual(True, False)

    def test_commcareapihandlerpull_last_job_success_time_filepath(self):
        self.assertEqual(True, False)

    def test_commcareapihandlerpull_get_date_range(self):
        self.assertEqual(True, False)

    def test_commcareapihandlerpull_get_initial_parameters_for_data_type(self):
        self.assertEqual(True, False)

    def test_commcareapihandlerpull_store_in_s3(self):
        self.assertEqual(True, False)

    def test_commcareapihandlerpull_pull_data(self):
        self.assertEqual(True, False)

    def test_commcareapihandlerpull_save_run_time(self):
        self.assertEqual(True, False)

    def test_commcareapihandlerpull_pull_data_for_domain(self):
        self.assertEqual(True, False)
"""


class TestCommCareAPIHandlerPush(unittest.TestCase):
    def test_commcareapihandlerpush_filepath(self):
        print('*** Running test_commcareapihandlerpush_filepath ***')
        test_data = [
            {
                'name': 'test_path',
                'api': CommCareAPIHandlerPush(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'specifier': 'test_specifier'
                },
                'return_value': 'test_domain/payload/test_specifier/2024/01/01/00/',
                'expect_exception': False,
                'exception': None
            }
        ]

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
                    file_path = test_case['api'].filepath(
                        test_case['parameters']['specifier']
                    )
                    if test_case['return_value'] is None:
                        self.assertIsNone(file_path)
                    else:
                        self.assertEqual(
                            test_case['return_value'],
                            file_path
                        )
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))

    def test_commcareapihandlerpush_get_post_content(self):
        print('*** Running test_commcareapihandlerpush_get_post_content ***')
        test_data = [
            {
                'name': 'test_specifier_1',
                'api': CommCareAPIHandlerPush(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
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
                'api': CommCareAPIHandlerPush(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
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
                'api': CommCareAPIHandlerPush(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'specifier': 'test_specifier_missing'
                },
                'return_value': None,
                'expect_exception': False,
                'exception': None
            }
        ]

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
                    post_content = test_case['api']._get_post_content(
                        test_case['parameters']['specifier']
                    )
                    if test_case['return_value'] is None:
                        self.assertIsNone(post_content)
                    else:
                        self.assertListEqual(
                            test_case['return_value'],
                            post_content
                        )
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))

    def test_commcareapihandlerpush_push_data(self):
        print('*** Running test_commcareapihandlerpush_push_data ***')
        test_data = [
            {
                'name': 'test_specifier_1',
                'api': CommCareAPIHandlerPush(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'data_type': {
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
                'api': CommCareAPIHandlerPush(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'data_type': {
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
                'api': CommCareAPIHandlerPush(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'data_type': {
                        'version': 'test_version',
                        'name': 'test_name'
                    },
                    'specifier': 'test_specifier_missing'
                },
                'expect_exception': False,
                'exception': None
            }
        ]

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
                    test_case['api']._push_data(
                        test_case['parameters']['data_type'],
                        test_case['parameters']['specifier']
                    )
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))

    def test_commcareapihandlerpush_push_data_for_domain(self):
        print('*** Running test_commcareapihandlerpush_push_data_for_domain ***')
        test_data = [
            {
                'name': 'test_specifier_1',
                'api': CommCareAPIHandlerPush(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'data_type': {
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
                'api': CommCareAPIHandlerPush(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'data_type': {
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
                'api': CommCareAPIHandlerPush(
                    'test_domain',
                    'test_domain-api-key',
                    datetime.strptime(
                        '2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                ),
                'parameters': {
                    'data_type': {
                        'version': 'test_version',
                        'name': 'test_name'
                    },
                    'specifier': 'test_specifier_missing'
                },
                'expect_exception': False,
                'exception': None
            }
        ]

        for test_case in test_data:
            with self.subTest(test_case['name']):
                print(f'  Running subtest {test_case["name"]}')
                try:
                    test_case['api'].push_data_for_domain(
                        test_case['parameters']['data_type'],
                        test_case['parameters']['specifier']
                    )
                    self.assertFalse(test_case['expect_exception'])
                except Exception as e:
                    self.assertTrue(test_case['expect_exception'])
                    self.assertEqual(str(test_case['exception']), str(e))


if __name__ == '__main__':
    unittest.main()
