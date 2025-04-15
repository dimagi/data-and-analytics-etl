import boto3
import json
import requests

from requests.exceptions import JSONDecodeError
ssm_client = boto3.client('ssm')

class APIError(Exception):
    def __init__(self, message, error_code):
        super().__init__(message)
        self.error_code = error_code

def process_response(response, is_boto=False):
    # Error if needed with failure code and reason for a response object.
    # The python requests library is assumed to be default
    if is_boto:
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        if not (200 <= status_code < 300):
            raise Exception(f"Boto3 request failed! Code: {status_code}.")
    elif response.ok:
        return response.json()
    else:
        try:
            error = APIError(f"Request failed! Code: {response.status_code}. Reason: {response.reason}. Details: {response.json()}", response.status_code)
        except JSONDecodeError:
            error = APIError(f"Request failed! Code: {response.status_code}.", response.status_code)
        raise error

def get_value_from_parameter_store(param_name):
    print(f"Getting value from parameter store with name: {param_name}")
    response = ssm_client.get_parameter(Name=param_name)
    process_response(response, is_boto=True)
    return response['Parameter']['Value']

def get_api_token(domain, specifier=None):
    param_name = domain +  (("-" + specifier) if specifier else "")  + '-api-key'
    return get_value_from_parameter_store(param_name)

def put_value_parameter_store(param_name, param_value, overwrite=False):
    response = ssm_client.put_parameter(Name=param_name, Value=param_value, Overwrite=overwrite)
    process_response(response, is_boto=True)

class APILimitCalculator(object):

    """
        This class helps CommCareAPIHandlerPull calcluate API limits for data types with an
        automatically-determined API limit (set via the "auto_determine_limit" parameter
        in the event payload).
    """

    # Snowflake pipeline can only handle file sizes 16MB or less
    max_file_size_in_mb = 16
    # File sizes often vary in size due to natural variety in size of cases, forms, etc. This offset
    #   provides room for instances where there are coincidentally large cases or forms in the request.
    file_size_grace_offset = 0.50
    # The maximum limit prevents calculated limits from being inappropriately high.
    max_limit = 10000

    @classmethod
    def determine_new_api_limit(cls, current_limit, size_of_test_request_in_bytes):
        """
            Calculates the appropriate API limit for this data type, based on the file size of
            a test request made to the API.
        """
        size_in_mb = size_of_test_request_in_bytes / 1000000
        print(f"Calculated file size with current limit to be: {size_in_mb}MB.")
        calculated_new_limit = cls.calculate_new_api_limit(size_in_mb, current_limit)
        if calculated_new_limit < cls.max_limit:
            print(f"New appropriate limit calculated to be: {calculated_new_limit}.")
            return calculated_new_limit
        else:
            print(f"New calculated limit was above the maximum ({cls.max_limit}). Using max limit instead...")
            return cls.max_limit

    @classmethod
    def calculate_new_api_limit(cls, size_in_mb, current_limit):
        """
            Example: if the maximum file size is 16MB, and a request with the current limit to the API returns a
            file size of 8 MB, the new limit is 16 / 8 * the current limit * the grace offset. The grace
            offset gives breathing room below the maxiumum file size so that a possibly larger request using the
            same limit does not go over the maximum file size.
        """
        new_appropriate_limit = (cls.max_file_size_in_mb / size_in_mb) * float(current_limit)
        new_limit = int(new_appropriate_limit * cls.file_size_grace_offset)
        return new_limit
