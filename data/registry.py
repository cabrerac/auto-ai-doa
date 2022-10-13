from pymongo import MongoClient
from botocore.exceptions import ClientError
import json
import networkx as nx
from .utils import *

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

        self._initialize_service_graph()

    def _initialize_service_graph(self):
        print("Initializing service graph from service registry....")
        self.service_graph = nx.DiGraph()

        #### Todo - read in from services table and add everything as nodes.

    def get_ancestors_and_self(self, service_name):
        """
        Self included.
        """
        ancestors = nx.ancestors(self.service_graph, service_name)
        ancestors.add(service_name)
        return ancestors
        
        ancestors = set()
        for p in self.service_graph.predecessors(service_name):
            ancestors.add(p)
            ancestors = ancestors | self.get_ancestors_and_self(p)
        ancestors.add(service_name)
        return ancestors

    def get_descendants_and_self(self, service_name):
        """
        Self included.
        """
        descendants = []
        for s in self.service_graph.sucessors(service_name):
            descendants.append(s)
            descendants = descendants + self.get_descendants(p)
        descendants.add(service_name)
        return set(descendants)

    def put_service(self, service_description):
        name = service_description['service_name']
        try:
            self.services_index_table.put_item(Item=service_description)
            print(f'resource, specify none      : write succeeded.')
        except Exception as e:
            print(f'resource, specify none      : write failed: {e}')
            return False
        self.service_graph.add_nodes_from([(name, service_description)])
        print("* Added node to the graph {} {} ".format(name, service_description))
        for i in service_description['inputs'].keys():
            self.service_graph.add_edge(i, name)
            print("**** Added edge to the graph {} -> {} ".format(i, name))
        
        self._print_graph()
        return True

    def _print_graph(self):
        print("\n********************* Nodes **************************")
        print(self.service_graph.nodes)
        print("********************* Edges **************************")
        print(self.service_graph.edges)
        print("******************************************************\n")


    def get_service(self, service_name, parameters):
        # print("Get Service {} {}".format(service_name, parameters))
        try:
            response = self.services_index_table.get_item(Key={'service_name': service_name})
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            # print("Service Response: {}".format(response))
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


    def get_data(self, dataId):
        try:
            response = self.data_index_table.get_item(Key={'dataId': dataId})
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            # print("Data Response: {}".format(response))
            if 'Item' in response:
                return response['Item']
            else:
                return None


    def make_data_id(self, service_name, dataHash):
        return service_name +  "/" + dataHash


    def build_data_description_from_task(self, service_name, input_hashes, output_hash, parameters, s3_bucket='climate-ensembling'):

        output_locations = {'output1': self.build_s3_location(output_hash, s3_bucket, service_name)}

        input_locations = {}
        if len(input_hashes) > 0:
            for input_name, hash in input_hashes.items():
                input_locations[input_name] = self.build_s3_location(hash, s3_bucket, input_name)

        data_description = {'dataId': self.make_data_id(service_name, output_hash),
                 'service_name': service_name, 
                 'inputs': input_locations,
                 'outputs': output_locations,
                 'parameters': parameters
        }

        return data_description

        
    def build_s3_location(self, hash, s3_bucket, service_name):
        return "s3://" + s3_bucket + "/" + service_name + "/" + hash

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