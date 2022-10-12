from modules.hypervisor import Hypervisor
from data.utils import get_required_parameters, make_hash
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
def parameters_individual():
    parameters_sl = {'SelectLocation': {'base_model': ['s3://climate-ensembling/EC-Earth3/'], 'models': ['TESTMODEL1', 'TESTMODEL2'], 'locations': ['Dhaka', 'Chicago']}}
    parameters_bc = {'BiasCorrection': {'bias_correction_methods': ['bc1', 'bc2'], 'thresholds': ['1', '2']}}
    parameters_cc = {'CalculateCost': {'time_windows': [['today', 'tomorrow'], [1980, 2020]]}}
    return (parameters_sl, {**parameters_sl,**parameters_bc}, {**parameters_sl, **parameters_bc, **parameters_cc})

@pytest.fixture 
def parameters_together():
    parameters_sl = {'SelectLocation': {'base_model': ['s3://climate-ensembling/EC-Earth3/'], 'models': ['TESTMODEL1', 'TESTMODEL2'], 'locations': ['Dhaka', 'Chicago']}}
    parameters_bc = {'BiasCorrection': {'bias_correction_methods': ['bc1', 'bc2'], 'thresholds': ['1', '2']}}
    parameters_cc = {'CalculateCost': {'time_windows': [['today', 'tomorrow'], [1980, 2020]]}}
    return {"service_name": "CalculateCost", "parameters": {**parameters_sl, **parameters_bc, **parameters_cc}}
    
# def test_basic_hashing(hypervisor, service_names, parameters_individual):
#     for service_name, p in zip(service_names, parameters_individual):
#         data_hash1 = hypervisor.registry.data_item_hash(service_name, p)
#         data_hash2 = hypervisor.registry.data_item_hash(service_name, p)

#         print("hash 1 {} \t hash 2 {}".format(data_hash1, data_hash2))
#         assert data_hash1 == data_hash2

# def test_get_required_parameters(register_services, service_names, parameters_individual):
#     hypervisor, _ = register_services
#     sl, bc, cc = service_names
#     parameters_sl, parameters_bc, parameters_cc = parameters_individual
#     for service_name, p in zip(service_names, parameters_individual):
#         ancestors = hypervisor.registry.get_ancestors_and_self(service_name)
#         subset_parameters = get_required_parameters(ancestors, p, hypervisor.registry.service_graph)
#         assert subset_parameters == p
        
#         data_hash1 = hypervisor.registry.data_item_hash(service_name, p)
#         data_hash2 = hypervisor.registry.data_item_hash(service_name, subset_parameters)
#         assert data_hash1 == data_hash2



import itertools

def test_expanded_service_sets(register_services, service_names, parameters_together):
    
    hypervisor, _ = register_services
    sl, bc, cc = service_names

    print("All parameters: {}".format(parameters_together))

    for sn in service_names:
        print("Service: {} Ancestors: {}\n".format(sn, hypervisor.registry.get_ancestors_and_self(sn)))
    
    s_e = build_expanded_service_sets(parameters_together['parameters'])
    overall_tasks_to_run = 1
    print("Expanded Service Sets {}".format(s_e))

    for expansion in s_e.values():
        overall_tasks_to_run = len(expansion) * overall_tasks_to_run


    print("Expanded Service Sets {}".format(s_e))


    print("\Actual length of service combos: {}".format(overall_tasks_to_run))


    assert overall_tasks_to_run == 32



def data_item_hash(p):
    """
    Makes a hash out of a service name and all (and only) the required parameters
    to compute an instance of running that service for one task.

    """

    s2 = json.dumps(p, sort_keys=True)
    s3 = repr(s2)
    s4 = make_hash(s3)
    # s4 = hash(s3)
    s5 = str(s4)
    return s5

def compute_task(task, level):
    h = data_item_hash(task)
    print("Computing task *{}* {} at level {}".format(h, task, level))


def test_construct_tasks_in_order(register_services, service_names, parameters_together):
    hypervisor, _ = register_services
    sl, bc, cc = service_names

    print("All parameters: {}".format(parameters_together))

    for sn in service_names:
        print("Service: {} Ancestors: {}\n".format(sn, hypervisor.registry.get_ancestors_and_self(sn)))

    s_e = build_expanded_service_sets(parameters_together['parameters'])

    most_child_service = cc

    
    preds = build_predecessor_level_dict(hypervisor.registry.service_graph, most_child_service)
    print("Full Predecessor chain: {}".format(preds))
    print("Service sets expanded (keys): {}".format(s_e.keys()))

    tasks_by_level = build_tasks_per_level(preds, s_e)

    for level, tasks in tasks_by_level.items():
        print("Level {} has {} tasks. Example: {}".format(level, len(tasks), tasks[0]))

    compute_tasks_by_level(tasks_by_level)

    assert False