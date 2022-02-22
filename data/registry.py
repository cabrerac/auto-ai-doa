from pymongo import MongoClient
from botocore.exceptions import ClientError

"""
Input - A piece of data that the hypervisor knows how to compute and is the output of another service. This is optionally specified only if the user want's to override the default value for the given parameters.
Parameter - A required piece of information, such as the location of the (most) parent data, or configuration for the compute nodes.

"""

class Registry:
    def __init__(self, client):
        pass

    def put_service(self, service_description):
        pass

    def get_service(self, service_description):
        return True

    def put_data(self, data_description):
        pass 

    def get_data(self, data_description):
        pass

REGION = "eu-west-1"
DATA_REGISTRY_NAME = 'climate-data-index'
SERVICES_REGISTRY_NAME = 'climate-services-index'
READ_CAPACITY = 5
WRITE_CAPACITY = 5

class DynamoDBRegistry(Registry):
    def __init__(self, client):
        self.client = client
        self.data_index_table = self.client.Table(DATA_REGISTRY_NAME)
        self.services_index_table = self.client.Table(SERVICES_REGISTRY_NAME)

    def put_service(self, service_description):
        
        try:
            self.services_index_table.put_item(Item=service_description)
            print(f'resource, specify none      : write succeeded.')
        except Exception as e:
            print(f'resource, specify none      : write failed: {e}')
        return True

    def get_service(self, service_name, parameters):
        print("Get Service {} {}".format(service_name, parameters))
        try:
            response = self.services_index_table.get_item(Key={'name': service_name})
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Service Response: {}".format(response))
            if 'Item' in response:
                return response['Item']
            else:
                return None

    def put_data(self, data_description):
        try:
            self.data_index_table.put_item(Item=data_description)
            print(f'resource, specify none      : write succeeded.')
        except Exception as e:
            print(f'resource, specify none      : write failed: {e}')
        return True


    def get_data(self, data_name, parameters):
        try:
            response = self.data_index_table.get_item(Key={'name': data_name})
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Data Response: {}".format(response))
            if 'Item' in response:
                return response['Item']
            else:
                return None



class MongoDBRegistry(Registry):
    def __init__(self, client):
        self.client = client
        self.services_collection = self.client.registry.services
        self.data_collection = self.client.registry.data

    def put_service(self, service_description):
        result = self.services_collection.insert_one(service_description)
        print("Registered service description: " + str(result.inserted_id) + " for service " + service_description["name"])
        return True

    def get_service(self, service_description):
        return True

    def put_data(self, data_description):
        result = self.data_collection.insert_one(data_description)
        print("Registered data description: " + str(result.inserted_id) + " for service " + data_description["name"] + " at output location " + data_description['output'])
        return True

    def get_data(self, data_description):
        return True