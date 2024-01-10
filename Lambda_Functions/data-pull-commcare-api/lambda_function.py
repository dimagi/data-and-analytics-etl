from datetime import datetime
import json
import os
# from botocore.vendored import requests

from CommCareAPIHandler import CommCareAPIHandlerPull
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

    print(f"Processing domain: {domain}...")
    api_token_for_domain = get_api_token(domain)
    print("Got API token for domain.")
    CommCareAPIHandlerPull(domain, api_token_for_domain, event_time, request_limit=1000).pull_data_for_domain()
    print(f"Data pull for domain: {domain} finished.")

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'CommCare to S3 data pull successful.'})
    }