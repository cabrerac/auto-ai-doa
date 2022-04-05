from modules.hypervisor import Hypervisor
from data.utils import get_required_parameters
import pytest
import boto3
import json
from data.registry import DynamoDBRegistry
from testdata import *

@pytest.fixture
def register_services(hypervisor):
    responses = []
    for sdesc in SERVICE_DESCRIPTIONS:
        with open(sdesc) as f:
            service_request = json.load(f)
            responses.append(hypervisor.register_service(service_request))
    return (hypervisor, responses)


@pytest.fixture
def ddb_client():
    ddb_client = boto3.resource("dynamodb")
    return ddb_client

@pytest.fixture
def ddb_registry(ddb_client):
    return DynamoDBRegistry(ddb_client)

@pytest.fixture
def s3_client():
    return boto3.client("s3")

@pytest.fixture
def ecs_client():
    return boto3.client("ecs")

@pytest.fixture
def hypervisor(s3_client, ecs_client, ddb_registry):
    hypervisor = Hypervisor(registry=ddb_registry, s3_client=s3_client, ecs_client=ecs_client)
    return hypervisor

@pytest.fixture 
def service_names():
    return "SelectLocation", "BiasCorrection", "CalculateCost"    

@pytest.fixture 
def parameters():
    parameters_sl = {'SelectLocation': {'base_model': 's3://climate-ensembling/EC-Earth3/', 'models': ['s3://climate-ensembling/EC-Earth3/', 's3://climate-ensembling/EC-Earth3/'], 'locations': ['Dhaka']}}
    parameters_bc = {'BiasCorrection': {'bias_correction_methods': ['bc1'], 'thresholds': ['1', '2']}}
    parameters_cc = {'CalculateCost': {'time_windows': [['today', 'tomorrow'], [1980, 2020]]}}
    return (parameters_sl, {**parameters_sl,**parameters_bc}, {**parameters_sl, **parameters_bc, **parameters_cc})
    
def test_basic_hashing(hypervisor, service_names, parameters):
    for service_name, p in zip(service_names, parameters):
        data_hash1 = hypervisor.registry.data_item_hash(service_name, p)
        data_hash2 = hypervisor.registry.data_item_hash(service_name, p)

        print("hash 1 {} \t hash 2 {}".format(data_hash1, data_hash2))
        assert data_hash1 == data_hash2

# def test_data_description(hypervisor, service_name, parameters):
#     data_hash = hypervisor.registry.data_item_hash(service_name, parameters)

#     service_registry_entry = {"service_name": "fdsaf"}
#     test_description = hypervisor.registry.build_data_description(service_name, service_registry_entry, data_hash, parameters)

#     print("Description {}".format(test_description))
#     assert False

def test_get_required_parameters(register_services, service_names, parameters):
    hypervisor, _ = register_services
    sl, bc, cc = service_names
    parameters_sl, parameters_bc, parameters_cc = parameters
    for service_name, p in zip(service_names, parameters):
        ancestors = hypervisor.registry.get_ancestors(service_name)
        ancestors.add(service_name)
        subset_parameters = get_required_parameters(ancestors, p, hypervisor.registry.service_graph)
        assert subset_parameters == p
        