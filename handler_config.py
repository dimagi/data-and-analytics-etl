main_bucket_name = 'commcare-snowflake-data-sync'

parameter_store_base_url = 'http://localhost:2773/systemsmanager/parameters'

api_tokens_param_names = {
    # domain: param_name
    'co-carecoordination-dev': 'ush-dev-ops-analytics-test-api-key'
}

# Information and settings regarding the CC API for each data type 
data_types = {
    'case': {
        'name': 'case',
        'version': 'v0.5',
        'limit': 5000,
        'uses_indexed_on': True
    },
    'form': {
        'name': 'form',
        'version': 'v0.5',
        'limit': 1000,
        'uses_indexed_on': True
    },
    'action_times': {
        'name': 'action_times',
        'version': 'v0.5',
        'limit': 1000
    },
    'location': {
        'name': 'location',
        'version': 'v0.5',
        'limit': 100
    },
    'fixture': {
        'name': 'fixture',
        'version': 'v0.5',
        'limit': 1000
    },
    'web-user': {
        'name': 'web-user',
        'version': 'v0.5',
        'limit': 1000
    },
    
}