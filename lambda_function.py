from collections import namedtuple
from datetime import datetime
import json

from CommCareAPIHandler import CommCareAPIHandlerPull, CommCareAPIHandlerPush
from util import (
    get_api_token,
)

# Date format: %Y-%m-%dT%H:%M:%S.%fZ
DateRangeTuple = namedtuple('DateRangeTuple', ['start_time', 'end_time'])

def err(msg):
    print(f"Error: {msg}")
    exit()
    return {
            'statusCode': 400,
            'body': json.dumps({'message': msg})
        }

def lambda_handler(event, context):
    event_time = datetime.now()
    print(f"Loaded current event time: {event_time}.")

    # Load domain passed in by Eventbridge
    domain = event['domain']

    print(f"Processing domain: {domain}...")
    api_token_for_domain = get_api_token(domain)
    print("Got API token for domain.")
    
    if 'operation_type' not in event:
        return err('Operation type was not specified in event data.')

    test_mode = False
    if 'test_mode' in event:
        test_mode = bool(event['test_mode'])

    ## -- S3 to CommCare
    if event['operation_type'] == 'cc_to_s3':

        # Custom date range processing
        if 'custom_date_range' in event:
            custom_date_range_config = event['custom_date_range']
            custom_date_range_tuple = DateRangeTuple(datetime.strptime(custom_date_range_config['start_time'], "%Y-%m-%dT%H:%M:%S.%fZ"),
                datetime.strptime(custom_date_range_config['end_time'], "%Y-%m-%dT%H:%M:%S.%fZ"))
            print(f"Specific date range specified. Details: {custom_date_range_tuple}")
        else:
            custom_date_range_tuple = None
        
        if 'api_info' not in event:
            return err('api_details was missing in event data.')
    
        CommCareAPIHandlerPull(domain, api_token_for_domain, event_time, request_limit=1000,
            custom_date_range_config=custom_date_range_tuple, test_mode=test_mode).pull_data_for_domain(event['api_info'])
        
        print(f"Data pull for domain: {domain} finished.")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'CommCare to S3 data pull successful.'})
        }

    ## -- CommCare to S3
    elif event['operation_type'] == 's3_to_cc':
        if 'api_info' not in event or 'specifiers' not in event:
            return err('api_info or specifiers were missing in event data.')
        specifiers = event['specifiers']
        for specifier in specifiers:
            api_token_for_domain = get_api_token(domain, specifier=specifier)
            CommCareAPIHandlerPush(domain, api_token_for_domain, event_time, request_limit=1000, test_mode=test_mode).push_data_for_domain(event['api_info'], specifier)

        print(f"Data push for domain: {domain} finished.")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'S3 to CommCare data push successful.'})
        }

    else:
        return err("Invalid operation_type was provided.")
    