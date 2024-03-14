import boto3
import importlib
import requests
import json
import unittest
import unittest.mock

from datetime import datetime
from unittest.mock import MagicMock
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
            'objects': {}
        }
    }
)

boto3.client = MagicMock(side_effect=get_boto3_client_mock)
requests.get = MagicMock(side_effect=mock_get)
requests.post = MagicMock(side_effect=mock_request)
json.load = MagicMock(side_effect=fake_json_file_load)

import CommCareAPIHandler
importlib.reload(CommCareAPIHandler)
from CommCareAPIHandler import CommCareAPIHandlerPull


# TODO: Implement CommCareAPIHandlerPull tests
"""
class TestCommCareAPIHandlerPull(unittest.TestCase):
    def test_commcareapihandlerpull_filepath(self):
        test_data = [
            {
                'name': None,
                'expect_exception': None,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            return

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpull_get_last_job_success_time(self):
        test_data = [
            {
                'name': None,
                'expect_exception': None,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            return

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpull_last_job_success_time_filepath(self):
        test_data = [
            {
                'name': None,
                'expect_exception': None,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            return

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpull_get_date_range(self):
        test_data = [
            {
                'name': None,
                'expect_exception': None,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            return

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpull_get_initial_parameters_for_data_type(self):
        test_data = [
            {
                'name': None,
                'expect_exception': None,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            return

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpull_store_in_s3(self):
        test_data = [
            {
                'name': None,
                'expect_exception': None,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            return

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpull_pull_data(self):
        test_data = [
            {
                'name': None,
                'expect_exception': None,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            return

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpull_save_run_time(self):
        test_data = [
            {
                'name': None,
                'expect_exception': None,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            return

        run_test_cases(self, test_data, test_function)

    def test_commcareapihandlerpull_pull_data_for_domain(self):
        test_data = [
            {
                'name': None,
                'expect_exception': None,
                'exception': None
            }
        ]

        def test_function(self, test_case):
            return

        run_test_cases(self, test_data, test_function)
"""


if __name__ == '__main__':
    unittest.main()
