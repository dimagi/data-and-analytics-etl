from testing.util import MockResponse
from const import POST


def mock_get(url, headers, params):
    json_data = {
        'meta': {
            'total_count': 1,
            'limit': 1,
            'next': ''
        },
        'objects': [
            {
                'indexed_on': '2024-01-01T00:00:00.000'
            }
        ]
    }
    response = MockResponse(json_data=json_data)
    return response


def mock_request(method, url, headers, json):
    json_data = {}
    if method == 'POST':
        json_data = {
            'form_id': 'test'
        }
    response = MockResponse(json_data=json_data)
    return response
