from pymongo import MongoClient
from botocore.exceptions import ClientError
import json
import networkx
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
        self.service_graph = networkx.DiGraph()

        #### Todo - read in from services table and add everything as nodes.

    def get_ancestors(self, service_name):
        """
        Self included.
        """
        ancestors = set()
        for p in self.service_graph.predecessors(service_name):
            ancestors.add(p)
            ancestors = ancestors | self.get_ancestors(p)
        ancestors.add(service_name)
        return ancestors

    def get_descendants(self, service_name):
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


    ### Climate Specific Stuff ###
    def make_data_id(self, service_name, dataHash):
        return service_name +  "-" + dataHash

    def _custom_hash(parameters):
        return "fdsaf"

    def data_item_hash(self, service_name, required_parameters):
        """
        Makes a hash out of a service name and all (and only) the required parameters
        to compute an instance of running that service for one task.

        """

        s1 = {'service_name': service_name, **required_parameters}
        s2 = json.dumps(s1, sort_keys=True)
        s3 = repr(s2)
        s4 = make_hash(s3)
        # s4 = hash(s3)
        s5 = str(s4)
        return s5

    def build_data_description(self, service_name, service, dataHash, parameters, s3_bucket='climate-ensembling'):
        # uuid = UUID.uuid.uuid4().hex
        ## TODO
        dataId = self.make_data_id(service_name, dataHash)
        input_locations = {}
        output_locations = {}
        # print("Node {} preds {}".format(self.service_graph.nodes[service_name], self.service_graph.predecessors(service_name)))
        for pname in self.service_graph.predecessors(service_name):
            p = self.service_graph.nodes[pname]
            # print("p {}".format(p))
            name = p['service_name']
            sub_p = get_required_parameters(self.service_graph.predecessors(name), parameters, self.service_graph)
            p_hash = self.data_item_hash(name, sub_p)
            input_locations[name] = "s3://" + s3_bucket + "/" + name + "/" + p_hash

        ancestors = self.get_ancestors(service_name)
        subset_parameters = get_required_parameters(ancestors, parameters, self.service_graph)
        s_hash = self.data_item_hash(service_name, subset_parameters)
        output_locations[service_name] = "s3://" + s3_bucket + "/" + service_name + "/" +  s_hash
        
        data_description = {'dataId': dataId,
                 'service_name': service_name, 
                 'inputs': input_locations,
                 'outputs': output_locations,
                 'parameters': subset_parameters
        }
        # print("********************Data Description*************************\n {} \n ***********************************".format(data_description))
        return data_description



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