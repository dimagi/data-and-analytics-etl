import boto3
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
        if status_code != 200:
            raise Exception(f"Boto3 request failed! Code: {status_code}.")
    elif response.ok:
        return response.json()
    else:
        raise APIError(f"Request failed! Code: {response.status_code}. Reason: {response.reason}", response.status_code)

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
