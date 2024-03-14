import json


class Boto3ClientMock():

    def __init__(
        self,
        parameters={},
        objects={}
    ):
        self.parameters = parameters
        self.objects = objects

    def get_parameter(self, Name):
        if (Name not in self.parameters):
            return {
                'ResponseMetadata': {
                    'HTTPStatusCode': 400
                }
            }
        else:
            return {
                'ResponseMetadata': {
                    'HTTPStatusCode': 200
                },
                'Parameter': {
                    'Value': self.parameters[Name]
                }
            }

    def put_parameter(self, Name, Value, Overwrite=False):
        if (Name in self.parameters and not Overwrite):
            return {
                'ResponseMetadata': {
                    'HTTPStatusCode': 400
                }
            }
        else:
            self.parameters[Name] = Value
            return {
                'ResponseMetadata': {
                    'HTTPStatusCode': 200
                }
            }

    def get_object(self, Bucket, Key):
        if Bucket not in self.objects:
            return {
                'ResponseMetadata': {
                    'HTTPStatusCode': 400
                }
            }
        else:
            record = None
            for object in self.objects[Bucket]:
                if object['Key'] == Key:
                    record = object
            if record is None:
                return {
                    'ResponseMetadata': {
                        'HTTPStatusCode': 400
                    }
                }
            else:
                return {
                    'ResponseMetadata': {
                        'HTTPStatusCode': 200
                    },
                    'ContentLength': 2,
                    'Key': record['Key'],
                    'Body': record['Body']
                }

    def put_object(self, Body, Bucket, Key):
        if (Bucket not in self.objects):
            return {
                'ResponseMetadata': {
                    'HTTPStatusCode': 400
                }
            }
        else:
            self.objects[Bucket].append(
                {
                    'Key': Key,
                    'Body': json.loads(Body)
                }
            )
            return {
                'ResponseMetadata': {
                    'HTTPStatusCode': 200
                }
            }

    def list_objects(self, Bucket, Prefix=None):
        if (Bucket not in self.objects):
            return {
                'ResponseMetadata': {
                    'HTTPStatusCode': 400
                }
            }
        elif Prefix is None:
            if len(self.objects[Bucket]) > 1:
                return {
                    'ResponseMetadata': {
                        'HTTPStatusCode': 200
                    },
                    'Contents': self.objects[Bucket]
                }
            else:
                return {
                    'ResponseMetadata': {
                        'HTTPStatusCode': 200
                    }
                }
        else:
            contents = [item for item in self.objects[Bucket] if item['Key'].startswith(Prefix)]
            if len(contents) > 1:
                return {
                    'ResponseMetadata': {
                        'HTTPStatusCode': 200
                    },
                    'Contents': contents
                }
            else:
                return {
                    'ResponseMetadata': {
                        'HTTPStatusCode': 200
                    }
                }
