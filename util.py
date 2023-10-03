import boto3
from handler_config import parameter_store_base_url, api_tokens_param_names
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
    response = ssm_client.get_parameter(Name=param_name)
    process_response(response, is_boto=True)
    return response['Parameter']['Value']

def get_api_token(domain):
    return get_value_from_parameter_store(api_tokens_param_names[domain])

def put_value_parameter_store(param_name, param_value, overwrite=False):
    response = ssm_client.put_parameter(Name=param_name, Value=param_value, Overwrite=overwrite)
    process_response(response, is_boto=True)

# def RequestLimiter():
#     def __init__(self, max_requests=max_requests):
#         self.max_requests = max_requests
#         self.request_count = 0
#         # self.period = period

#     def execute_if_under_limit(self, request):
#         if self.request_count < max_requests:
#             return request()
#         else:
#             raise Exception("Request limit reached.")
        
        
    
