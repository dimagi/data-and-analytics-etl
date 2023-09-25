import boto3
from botocore.exceptions import ClientError
from handler_config import main_bucket_name, data_types
from datetime import datetime
import json

import requests
from util import APIError, process_response

s3 = boto3.client('s3')

class CommCareAPIHandler:
    def __init__(self, domain, api_token_for_domain, event_time, request_limit=100):
        self.domain = domain
        self.api_token = api_token_for_domain
        self.event_time = event_time
        self.request_count = 0
        self.request_limit = request_limit
        self.APIErrorCount = 0
        self.APIErrorMax = 3

    def __str__(self):
        attribute_strings = [f"{key}={value}" for key, value in vars(self).items()]
        return f"{self.__class__.__name__}({', '.join(attribute_strings)})"

    def filepath(self, data_type):
        return f"""{self.domain}/snowflake-copy/{data_type}-test/{self.event_time.strftime('%Y')}/{self.event_time.strftime('%m')}/{self.event_time.strftime('%d')}/{self.event_time.strftime('%H')}/"""

    def api_base_url(self, data_type):
        return f"https://www.commcarehq.org/a/{self.domain}/api/{data_type['version']}/{data_type['name']}/"

    # NOTE: Needs memoization
    def _get_last_job_success_time(self, data_type_name):
        print(f"Loading last successful job time for {data_type_name} run on domain {self.domain}...")
        try:
            last_successful_job_time = s3.get_object(Bucket=main_bucket_name, Key=self._last_job_success_time_filepath(data_type_name))['Body'].read().decode("utf-8")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print("No last successful job time was provided via the txt file. Exiting...")
            raise
        print("Load successful.")
        return last_successful_job_time

    # NOTE: needs memoization
    def _last_job_success_time_filepath(self, data_type_name):
        return f"{self.domain}/snowflake-copy/{data_type_name}-test/last_successful_job_time.txt"

    def get_initial_parameters_for_data_type(self, data_type):
        params = {
            'limit': data_type['limit']
        }
        start_time = self._get_last_job_success_time(data_type['name'])
        end_time = self.event_time.isoformat()
        if data_type.get('uses_indexed_on'):
            # v0.6
            # if data_type['name'] == 'case':
            #     params.update({
            #         'indexed_on.gt': start_time,
            #         'indexed_on.lt': end_time,
            #     })
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

    def pull_data(self, data_type_name):
        data_type = data_types[data_type_name]
        params = self.get_initial_parameters_for_data_type(data_type)
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
            if params['limit'] < count:
                if data_type.get('uses_indexed_on'):
                    last_item = response_data['objects'][limit - 1]
                    request_end_boundary = datetime.strptime(last_item['indexed_on'], "%Y-%m-%dT%H:%M:%S.%fZ").isoformat()
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
                filename = f"{data_type_name}_{self._get_last_job_success_time(data_type_name)}_{self.event_time.isoformat()}_{data_type_request_count}.json"
            if count:
                self.store_in_s3(data_type, response_data, filename)
    
        print(f"Data type {data_type_name} processing for domain: {self.domain} finished. API handler has made {self.request_count} requests in total.")
        self._save_run_time(data_type_name, self.event_time.isoformat())

    def _save_run_time(self, data_type_name, time):
        print(f"Saving run time of {data_type_name} pull on domain {self.domain} with filename: {self._last_job_success_time_filepath(data_type_name)}...")
        s3.put_object(Body=str(time), Bucket=main_bucket_name, Key=self._last_job_success_time_filepath(data_type_name))
        print(f"Run time saved.")

    def pull_data_for_domain(self):
        for data_type_name in data_types.keys():
            try:
                self.pull_data(data_type_name)
            except APIError as e:
                print({
                    "ERROR making request to API": str(e),
                    "domain": self.domain,
                    "data_type": data_type_name
                })
                self.APIErrorCount +=1
                if self.APIErrorCount >= self.APIErrorMax:
                    raise
    