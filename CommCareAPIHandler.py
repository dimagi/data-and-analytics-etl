import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import json
import requests
from const import CASE
from util import APIError, process_response

main_bucket_name = 'commcare-snowflake-data-sync'
base_commcare_url = 'https://www.commcarehq.org'
base_staging_url = 'https://staging.commcarehq.org'

s3 = boto3.client('s3')

class CommCareAPIHandler:
    def __init__(self, is_staging, domain, api_token_for_domain, event_time, request_limit=100, custom_date_range_config=None, test_mode=False, use_lag=False):
        self.is_staging = is_staging
        self.domain = domain
        self.api_token = api_token_for_domain
        self.event_time = event_time
        self.request_count = 0
        self.request_limit = request_limit
        self.APIErrorCount = 0
        self.APIErrorMax = 3
        self.custom_date_range_config = custom_date_range_config
        self.test_mode = test_mode

    def __str__(self):
        attribute_strings = [f"{key}={value}" for key, value in vars(self).items()]
        return f"{self.__class__.__name__}({', '.join(attribute_strings)})"

    def api_base_url(self, data_type):
        request_domain = self.domain
        if self.is_staging:
            domain_url = base_staging_url
            request_domain = request_domain.replace('staging-', '')
        else:
            domain_url = base_commcare_url
        return f"{domain_url}/a/{request_domain}/api/{data_type['version']}/{data_type['name']}/"

    def api_call_headers(self):
        return {'Content-Type':'application/json', 'Authorization' : f'ApiKey {self.api_token}'}

    def _perform_method(self, method, *args):
        try:
            method(*args)
        except APIError as e:
            print({
                "ERROR making request to API": str(e),
                "domain": self.domain,
                "data_type": args[0]
            })
            self.APIErrorCount +=1
            if self.APIErrorCount >= self.APIErrorMax:
                raise


class CommCareAPIHandlerPull(CommCareAPIHandler):

    # Snowflake pipeline can only handle file sizes 16MB or less
    max_file_size_in_mb = 16
    # File sizes often vary in size due to natural variety in size of cases, forms, etc. This offset
    #   provides room for instances where there are coincidentally large cases or forms in the request.
    file_size_grace_offset = 0.50

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if kwargs['use_lag']:
            self.event_time = self.event_time - timedelta(hours=0, minutes=5)
            print("Added a 5 minute lag.")

    def filepath(self, data_type):
        path_beginning = f"{self.domain}/snowflake-copy/"
        return path_beginning + data_type + ("-test" if self.test_mode else "") + f"/{self.event_time.strftime('%Y')}/{self.event_time.strftime('%m')}/{self.event_time.strftime('%d')}/{self.event_time.strftime('%H')}/"

    def _get_stored_param_filepath(self, stored_parameter_name, data_type_name):
        return f"{self.domain}/snowflake-copy/{data_type_name}" + ("-test" if self.test_mode else "") + f"/{stored_parameter_name}.txt"

    def _get_stored_param_from_s3(self, stored_parameter_name, data_type_name):
        return s3.get_object(Bucket=main_bucket_name, Key=self._get_stored_param_filepath(stored_parameter_name, data_type_name))['Body'].read().decode("utf-8")

    def _get_last_job_success_time(self, data_type_name):
        print(f"Loading last successful job time for {data_type_name} run on domain {self.domain}...")
        last_successful_job_time = self._get_stored_param_from_s3('last_successful_job_time', data_type_name)
        print(f"Load successful. Last successful job time was: {last_successful_job_time}.")
        return last_successful_job_time

    def _get_current_api_limit(self, data_type_name):
        print(f"Loading current API limit for {data_type_name} run on domain {self.domain}...")
        api_limit = self._get_stored_param_from_s3('api_limit', data_type_name)
        print(f"Load successful. Current API limit is: {api_limit}.")
        return api_limit

    def _save_run_time(self, data_type_name, time):
        filepath = self._get_stored_param_filepath('last_successful_job_time', data_type_name)
        print(f"Saving run time of {data_type_name} pull on domain {self.domain} with filename: {filepath}. Run time: {str(time)}...")
        s3.put_object(Body=str(time), Bucket=main_bucket_name, Key=filepath)
        print(f"Run time saved.")

    def _save_api_limit(self, data_type_name, limit):
        filepath = self._get_stored_param_filepath('api_limit', data_type_name)
        print(f"Saving new API limit of {data_type_name} pull on domain {self.domain} with filename: {filepath}...")
        s3.put_object(Body=str(limit), Bucket=main_bucket_name, Key=filepath)
        print(f"API limit saved. New value: {limit}.")

    def _get_api_limit(self, data_type, start_time, end_time):
        print(f"Verifying api limit for {data_type['name']} run on domain {self.domain}...")
        current_limit = data_type['limit']
        try:
            current_limit = self._get_current_api_limit(data_type['name'])
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"No stored api limit found (no .txt file).")
            else:
                raise
        new_limit = self._determine_new_api_limit(data_type, current_limit, start_time, end_time)
        self._save_api_limit(data_type['name'], new_limit)
        return new_limit

    def _determine_new_api_limit(self, data_type, current_limit, start_time, end_time):
        print(f"Testing current api limit: {current_limit}...")
        api_url = self.api_base_url(data_type)
        params = {
            "limit": current_limit
        }
        params = self._get_indexing_params(params, data_type, start_time, end_time)
        print(f"Making request to URL: {api_url} with parameters: {params}.")
        response = requests.get(api_url, headers=self.api_call_headers(), params=params)
        response_data = process_response(response)
        size_in_bytes = len(json.dumps(response_data).encode('utf-8'))
        size_in_mb = size_in_bytes / 1000000
        print(f"Calculated file size with current limit to be: {size_in_mb}MB.")
        return self._calculate_new_api_limit(size_in_mb, current_limit)

    def _calculate_new_api_limit(self, size_in_mb, current_limit):
        new_appropriate_limit = (self.max_file_size_in_mb / size_in_mb) * float(current_limit)
        new_limit = int(new_appropriate_limit * self.file_size_grace_offset)
        print(f"New appropriate limit calculated to be: {new_limit}.")
        return new_limit

    def get_date_range(self, data_type):
        if self.custom_date_range_config:
            return (self.custom_date_range_config.start_time.isoformat(), self.custom_date_range_config.end_time.isoformat())
        else:
            return (self._get_last_job_success_time(data_type['name']), self.event_time.isoformat())

    def get_initial_parameters_for_data_type(self, data_type, start_time, end_time):
        params = {}
        if data_type.get('auto_determine_limit'):
            params.update({
                'limit': self._get_api_limit(data_type, start_time, end_time)
            })
        else:
            params.update({
                'limit': data_type['limit']
            })
        params = self._get_indexing_params(params, data_type, start_time, end_time)
        return params

    def _get_indexing_params(self, params, data_type, start_time, end_time):
        if data_type.get('uses_indexed_on'):
            if data_type['name'] == 'form':
                params.update({
                    'include_archived': 'true',
                })
            params.update({
                'order_by': 'indexed_on',
                'indexed_on_start': start_time,
                'indexed_on_end': end_time,
            })
        else:
            if data_type['name'] == 'action_times':
                params.update({
                    'UTC_start_time_start': start_time,
                    'UTC_start_time_end': end_time,
                })
        return params
    
    def store_in_s3(self, cc_api_data_type, response_data, filename):
        print(f"Storing file of {cc_api_data_type['name']} type with filename: {filename}...")
        s3.put_object(Body=json.dumps(response_data), Bucket=main_bucket_name, Key=(self.filepath(cc_api_data_type['name']) + filename))
        print(f"{cc_api_data_type['name']} file stored.")

    def pull_data(self, data_type):
        data_type_name = data_type['name']
        initial_start_time, initial_end_time = self.get_date_range(data_type)
        params = self.get_initial_parameters_for_data_type(data_type, initial_start_time, initial_end_time)
        api_url = self.api_base_url(data_type)
    
        print(f"Starting {data_type_name} processing for domain: {self.domain}. Storing in bucket: {main_bucket_name} with filepath: {self.filepath(data_type_name)}.")
        more_items_remain = True
        if not data_type.get('uses_indexed_on'):
            data_type_request_count = 0 # Record request count to add to filename if needed
        while more_items_remain:
            ## Make request
            print(f"Making request to URL: {api_url} with parameters: {params}.")
            if self.request_count < self.request_limit:
                response = requests.get(api_url, headers=self.api_call_headers(), params=params)
                self.request_count += 1
            else:
                raise Exception(f"Request limit reached for API Handler: {self}.")
            response_data = process_response(response)
            print(f"Request successful.")

            ## Prepare next request (if needed)
            if data_type.get('uses_indexed_on'):
                indexed_on_start_of_last_request = params.get('indexed_on_start')
            if response_data['meta']['next']:
                if data_type.get('uses_indexed_on'):
                    limit = response_data['meta']['limit']
                    last_item = response_data['objects'][limit - 1]
                    try:
                        request_end_boundary = datetime.strptime(last_item['indexed_on'], "%Y-%m-%dT%H:%M:%S.%fZ").isoformat()
                    except ValueError:
                        request_end_boundary = datetime.strptime(last_item['indexed_on'], "%Y-%m-%dT%H:%M:%S.%f").isoformat()
                    params['indexed_on_start'] = request_end_boundary
                    print(f"Continuing to next page, with new indexed_on start: {request_end_boundary}...")
                else:
                    cursor = response_data['meta']['next']
                    api_url = self.api_base_url(data_type) + cursor
                    params = None
            else:
                if data_type.get('uses_indexed_on'):
                    request_end_boundary = params.get('indexed_on_end')
                more_items_remain = False
                print(f"Reached end of {data_type_name} pagination.")

            ## Put data in S3
            if data_type.get('uses_indexed_on'):
                filename = f"{data_type_name}_{indexed_on_start_of_last_request}_{request_end_boundary}.json"
            else:
                data_type_request_count += 1
                filename = f"{data_type_name}_{initial_start_time}_{initial_end_time}_{data_type_request_count}.json"
            if len(response_data['objects']):
                self.store_in_s3(data_type, response_data, filename)
    
        print(f"Data type {data_type_name} processing for domain: {self.domain} finished. API handler has made {self.request_count} requests in total.")
        if not self.custom_date_range_config:
            self._save_run_time(data_type_name, self.event_time.isoformat())

    def pull_data_for_domain(self, api_details):
        for data_type_name in api_details.keys():
            try:
                self._perform_method(self.pull_data, api_details[data_type_name])
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print(f"Missing stored parameter (i.e. last successful job time, api limit) txt file. Skipping processing for data_type: {data_type_name}...")
                else:
                    raise

class CommCareAPIHandlerPush(CommCareAPIHandler):
    def filepath(self, specifier):
        path = f"""{self.domain}/payload/{specifier}/{self.event_time.strftime('%Y')}/{self.event_time.strftime('%m')}/{self.event_time.strftime('%d')}/{self.event_time.strftime('%H')}/"""
        return path

    def _get_request_content(self, specifier):
        full_path = self.filepath(specifier)
        print(f"S3 file path: {full_path}...")
        request_data_arr = []
        # Parse all files in filepath, add each to post_data_arr as json
        s3_objects_response = s3.list_objects(Bucket=main_bucket_name, Prefix=full_path)
        try:
            folder_contents = s3_objects_response['Contents']
        except KeyError:
            print("Folder not found. Likely that the data is intentionally empty.")
            return None
        for object_dict in folder_contents:
            obj = s3.get_object(Bucket=main_bucket_name, Key=object_dict['Key'])
            if not obj['ContentLength']:
                print("WARNING: Found an empty object in folder. This is normal if you created the folder manually in the AWS console.")
                continue
            request_data_arr.append(json.load(obj['Body']))
        return request_data_arr

    def _make_request(self, data, data_type_name, api_url, request_method):
        print(f"Data: {data}")
        response = requests.request(request_method, api_url, headers=self.api_call_headers(), json=data)
        response_data = process_response(response)
        print(f"{request_method} successful.")
        if data_type_name == CASE:
            print(f"Form ID: {response_data.get('form_id')}")
        print(f"Response data: {response_data}")

    def _push_data(self, data_type, specifier):
        data_type_name = data_type['name']
        print(f"**Beginning data push of data type {data_type_name} for domain: {self.domain}; specifier: {specifier}...")
        api_url = self.api_base_url(data_type)

        print(f"Getting data to inculde in request...")
        request_data_arr = self._get_request_content(specifier)
        if not request_data_arr:
            print("Could not find S3 data. Ending processing of data push...")
            return
        print("Content loaded from S3.")

        print(f"Starting requests to url: {api_url}")
        request_count = 1
        request_method = data_type['method']
        for data in request_data_arr:
            print(f"Making {request_method} request #{request_count} of {len(request_data_arr)}...")
            self._make_request(data, data_type_name, api_url, request_method)
            request_count += 1
        print(f"**All requests done. Processing finished for domain {self.domain}; specifier: {specifier}.")

    def push_data_for_domain(self, data_type, specifier):
         self._perform_method(self._push_data, data_type, specifier)
