from testing.boto3_mock import Boto3ClientMock


def fake_json_file_load(json_data):
    return json_data


def generate_get_boto3_client_mock_function(input_dict: dict):

    def get_boto3_client_mock(Name: str):
        input = input_dict[Name]
        parameters = input['parameters']
        objects = input['objects']
        return Boto3ClientMock(
            parameters=parameters,
            objects=objects
        )

    return get_boto3_client_mock


def run_test_cases(self, test_data, test_function):
    for test_case in test_data:
        with self.subTest(test_case['name']):
            print(f'  Running subtest {test_case["name"]}')
            try:
                test_function(self, test_case)
                self.assertFalse(test_case['expect_exception'])
            except Exception as e:
                self.assertTrue(test_case['expect_exception'], msg=str(e))
                self.assertEqual(str(test_case['exception']), str(e))


class MockResponse:
    def __init__(self, status_code=200, ok=True, reason=None, json_data={}):
        self.status_code = status_code
        self.ok = ok
        self.reason = reason
        self.json_data = json_data

    def json(self):
        return self.json_data
