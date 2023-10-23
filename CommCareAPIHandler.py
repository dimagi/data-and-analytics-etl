import boto3
from botocore.exceptions import ClientError
from handler_config import main_bucket_name, data_types
from datetime import datetime
import json

import requests
from util import APIError, process_response

s3 = boto3.client('s3')

class CommCareAPIHandler:
    def __init__(self, domain, api_token_for_domain, event_time, request_limit=100, custom_date_range_config=None, test_mode=False):
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
        return f"https://www.commcarehq.org/a/{self.domain}/api/{data_type['version']}/{data_type['name']}/"

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

    def filepath(self, data_type):
        path_beginning = f"{self.domain}/snowflake-copy/"
        if self.custom_date_range_config:
            return path_beginning + self.custom_date_range_config.custom_folder_name + "/"
        else:
            return path_beginning + data_type + ("-test" if self.test_mode else "") + f"/{self.event_time.strftime('%Y')}/{self.event_time.strftime('%m')}/{self.event_time.strftime('%d')}/{self.event_time.strftime('%H')}/"

    def _get_last_job_success_time(self, data_type_name):
        print(f"Loading last successful job time for {data_type_name} run on domain {self.domain}...")
        last_successful_job_time = s3.get_object(Bucket=main_bucket_name, Key=self._last_job_success_time_filepath(data_type_name))['Body'].read().decode("utf-8")
        print("Load successful.")
        return last_successful_job_time

    def _last_job_success_time_filepath(self, data_type_name):
        return f"{self.domain}/snowflake-copy/{data_type_name}" + ("-test" if self.test_mode else "") + "/last_successful_job_time.txt"

    def get_date_range(self, data_type):
        if self.custom_date_range_config:
            return (self.custom_date_range_config.start_time.isoformat(), self.custom_date_range_config.end_time.isoformat())
        else:
            return (self._get_last_job_success_time(data_type['name']), self.event_time.isoformat())

    def get_initial_parameters_for_data_type(self, data_type, start_time, end_time):
        params = {
            'limit': data_type['limit']
        }

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
        headers = {'Content-Type':'application/json', 'Authorization' : f'ApiKey {self.api_token}'}
    
        print(f"Starting {data_type_name} processing for domain: {self.domain}. Storing in bucket: {main_bucket_name} with filepath: {self.filepath(data_type_name)}.")
        more_items_remain = True
        if not data_type.get('uses_indexed_on'):
            data_type_request_count = 0 # Record request count to add to filename if needed
        while more_items_remain:
            print(f"Making request to URL: {api_url} with parameters: {params}.")
            if self.request_count < self.request_limit:
                response = requests.get(api_url, headers=headers, params=params)
                self.request_count += 1
            else:
                raise Exception(f"Request limit reached for API Handler: {self}.")
            response_data = process_response(response)
            print(f"Request successful.")
    
            if data_type.get('uses_indexed_on'):
                indexed_on_start_of_last_request = params.get('indexed_on_start')
            count = response_data['meta']['total_count']
            print(f"|{data_type_name} count from request: {count}")
            limit = response_data['meta']['limit']
            assert data_type['limit'] == limit
            if data_type['limit'] < count:
                if data_type.get('uses_indexed_on'):
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
    
            if data_type.get('uses_indexed_on'):
                filename = f"{data_type_name}_{indexed_on_start_of_last_request}_{request_end_boundary}.json"
            else:
                data_type_request_count += 1
                filename = f"{data_type_name}_{initial_start_time}_{initial_end_time}_{data_type_request_count}.json"
            if count:
                self.store_in_s3(data_type, response_data, filename)
    
        print(f"Data type {data_type_name} processing for domain: {self.domain} finished. API handler has made {self.request_count} requests in total.")
        if not self.custom_date_range_config:
            self._save_run_time(data_type_name, self.event_time.isoformat())

    def _save_run_time(self, data_type_name, time):
        print(f"Saving run time of {data_type_name} pull on domain {self.domain} with filename: {self._last_job_success_time_filepath(data_type_name)}...")
        s3.put_object(Body=str(time), Bucket=main_bucket_name, Key=self._last_job_success_time_filepath(data_type_name))
        print(f"Run time saved.")

    def pull_data_for_domain(self, api_details):
        for data_type_name in api_details.keys():
            try:
                self._perform_method(self.pull_data, api_details[data_type_name])
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print(f"No last successful job time was provided via the txt file. Skipping processing for data_type: {data_type_name}...")

class CommCareAPIHandlerPush(CommCareAPIHandler):
    def filepath(self, specifier):
        path = f"""{self.domain}/payload/{specifier}/{self.event_time.strftime('%Y')}/{self.event_time.strftime('%m')}/{self.event_time.strftime('%d')}/{self.event_time.strftime('%H')}/"""
        return path

    def _get_post_content(self, specifier):
        full_path = self.filepath(specifier)
        print(f"S3 file path: {full_path}...")
        post_data_arr = []
        # Parse all files in filepath, add each to post_data_arr as json
        s3_objects_response = s3.list_objects(Bucket=main_bucket_name, Prefix=full_path)
        try:
            folder_contents = s3_objects_response['Contents']
        except KeyError:
            print("Folder not found. Likely that the data is intentionally empty.")
            return None
        for object_dict in folder_contents:
            obj = s3.get_object(Bucket=main_bucket_name, Key=object_dict['Key'])
            post_data_arr.append(json.load(obj['Body']))
        return post_data_arr

    def _push_data(self, data_type, specifier):
        print(f"**Beginning data push of data type {data_type['name']} for domain: {self.domain}; specifier: {specifier}...")
        api_url = self.api_base_url(data_type)
        headers = {'Content-Type':'application/json', 'Authorization' : f'ApiKey {self.api_token}'}
        print(f"Getting content to POST...")
        post_data_arr = self._get_post_content(specifier)
        if not post_data_arr:
            print("Ending processing of data push...")
            return
        print("Content loaded from S3.")

        print(f"POSTing to url: {api_url}")
        request_count = 1
        for data in post_data_arr:
            print(f"Making request #{request_count} of {len(post_data_arr)}...")
            print(f"Data: {data}")
            response = requests.post(api_url, headers=headers, json=data)
            response_data = process_response(response)
            print("POST successful.")
            print(f"Form ID: {response_data.get('form_id')}")
            print(f"Response data: {response_data}")
            request_count += 1

        print(f"**All requests done. Processing finished for domain {self.domain}; specifier: {specifier}.")

    def push_data_for_domain(self, data_type, specifier):
         self._perform_method(self._push_data, data_type, specifier)
