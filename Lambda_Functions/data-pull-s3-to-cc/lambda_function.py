from datetime import datetime
import json

from CommCareAPIHandler import CommCareAPIHandlerPush
from util import (
    get_value_from_parameter_store,
    get_api_token,
    put_value_parameter_store
)

def lambda_handler(event, context):
    event_time = datetime.now()
    print(f"Loaded current event time: {event_time}.")

    # Load domain passed in by Eventbridge
    domain = event['domain']
    # Load path specifiers
    specifiers = event['specifiers']

    print(f"Processing domain: {domain}...")
    print("Got API token for domain.")
    for specifier in specifiers:
        api_token_for_domain = get_api_token(domain, specifier=specifier)
        CommCareAPIHandlerPush(domain, api_token_for_domain, event_time, request_limit=1000).push_data_for_domain('case', specifier)
    print(f"Data pull for domain: {domain} finished.")

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'CommCare to S3 data pull successful.'})
    }