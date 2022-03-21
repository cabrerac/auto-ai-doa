from re import sub
from uuid import UUID
from data.registry import DynamoDBRegistry, MongoDBRegistry, Registry
from pymongo import MongoClient
import boto3
import json
from data.utils import get_required_parameters

class Hypervisor():
    def __init__(self, backend="ddb", mongo_port=21017, base_bucket='climate-ensembling'):
        if backend=='ddb':
            self.ddb_client = boto3.resource("dynamodb")
            self.registry = DynamoDBRegistry(self.ddb_client)
        elif backend=='mongo':
            self.mongo = MongoClient(port=mongo_port)
            self.registry = MongoDBRegistry(self.mongo)
        
        self.s3_client = boto3.client("s3")
        self.ecs_client = boto3.client("ecs")
        self.BASE_BUCKET = base_bucket
        self.CLUSTER_NAME = "HypervisorCluster"

        response = self.ecs_client.create_cluster(clusterName=self.CLUSTER_NAME)
        # print(json.dumps(response, indent=4))

    def register_service(self, service_description):
        print("Register: Service description {}".format(service_description))
        registry_response = self.registry.put_service(service_description)

        return registry_response

    def execute_service(self, service_request):
        print("Execute: Service description {}".format(service_request))
        service_name = service_request['target_service']
        service_parameters = service_request['parameters']
        print("Parameters {} ".format(service_parameters))
        service_response = self._compute(service_name, service_parameters)
        return service_response

    def _compute(self, service_name, parameters):
        ancestors = self.registry.get_ancestors(service_name)
        subset_parameters = get_required_parameters(ancestors, parameters)
        dataHash = self.registry.data_item_hash(service_name, subset_parameters)
        dataId = self.registry.make_data_id(service_name, dataHash)
        cached_data = self.registry.get_data(dataId)
        if False:
        # if cached_data is not None or service_name in inputs:
            print("Returning cached data for {}: {}".format(service_name, cached_data))
            return cached_data
        else:
            waiting = True
            service = self.registry.get_service(service_name, parameters)
            sub_service_responses = []

            # Dispatch jobs or get the cached data for the required inputs
            for input in service['inputs']:
                sub_service_responses.append(self._compute(input, parameters))
            
            # Sync - Wait for all the services to complete before doing the upper service
            i=0
            while waiting:
                # TODO Make this better parallelized for computing all the models at once
                for service in sub_service_responses:
                    if self._is_service_completed(service):
                        sub_service_responses.remove(service)
                if len(sub_service_responses) == 0:
                    waiting = False
                if i % 1000 == 0:
                    print("Still waiting on services.... {}".format(i))
                    i=0
                i = i + 1

            return self._compute_single_service(service_name, service, parameters)

    def _compute_single_service(self, service_name, service, parameters):
        
        data_hash = self.registry.data_item_hash(service_name, parameters)
        data_description = self.registry.build_data_description(service_name, data_hash, parameters)
        print("Computing service {}".format(data_description))
        # Fill data location into registry
        if not self.registry.put_data(data_description):
            print("Data not registered successfully! {}".format(data_description))

        # Call docker
        taskDefinition = "ClimateEnsemblingManual"

        ## TODO pass parameters??
        task_response = self.ecs_client.run_task(
            taskDefinition=taskDefinition,
            launchType='FARGATE',
            cluster=self.CLUSTER_NAME,
            platformVersion='LATEST',
            count=1,
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [
                        'subnet-0183fe050d93b845a',
                    ],
                    'assignPublicIp': 'ENABLED',
                    'securityGroups': ["sg-0a6e23d1e06d90604"]
                }
            },
            overrides={'containerOverrides': [
                {
                    'name': 'climate',
                    'command': self._build_container_command(data_description)
                }
            ]}
        )
        print("ECS Task for hash {} running......".format(data_hash))
        return task_response

    def _build_container_command(self, parameters):
        cmd = "/bin/sh -c \"cd /app && ls -altr && pwd && python3 --version && pip freeze "
        cmd = cmd + " && " + "python3 /app/main.py "
        cmd = cmd + "--parameters='{}'".format(json.dumps(parameters)).replace("\"", "\\\"")
        cmd = cmd + "\""
        print("\nContainer Parameters: {} \n\n Command: {}".format(parameters, cmd))
        return [cmd]
