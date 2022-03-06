from re import sub
from data.registry import DynamoDBRegistry, MongoDBRegistry, Registry
from pymongo import MongoClient
import boto3
import json

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
        name  = service_description['name']
        image = service_description['image']


        # response = self.ecs_client.register_task_definition(
            
        #         containerDefinitions=[
        #             {
        #                 "name": name,
        #                 "image": image,
        #             }
        #         ],
        #         executionRoleArn="arn:aws:iam::288687564189:role/climate-container-role",
        #         networkMode="awsvpc",
        #         requiresCompatibilities= [
        #             "FARGATE"
        #         ],
        #         cpu= "256",
        #         memory= "512",        
        #         family= "ClimateEnsembling",
        #         )

        # print(json.dumps(response, indent=4, default=str))

        registry_response = self.registry.put_service(service_description)

        return registry_response

    def execute_service(self, service_request):
        print("Execute: Service description {}".format(service_request))
        service_name = service_request['name']
        override_inputs = service_request['inputs']
        service_parameters = service_request['parameters']
        combined_parameters = {**service_parameters, **override_inputs}
        print("Combined parameters {} ".format(combined_parameters))
        task_response = self._compute(service_name, override_inputs, service_parameters)
        return task_response

    def _compute(self, service_name, parameters):
        cached_data = self.registry.get_data(service_name, parameters)
        if False:
        # if cached_data is not None or service_name in inputs:
            print("Returning cached data for {}: {}".format(service_name, cached_data))
            return cached_data
        else:
            waiting = True
            service = self.registry.get_service(service_name, parameters)
            sub_task_responses = []

            # Dispatch jobs or get the cached data for the required inputs
            for input in service['inputs']:
                sub_task_responses.append(self._compute(input, parameters))
            
            # Sync - Wait for all the tasks to complete before doing the upper task

            i=0
            while waiting:
                # TODO Make this better parallelized for computing all the models at once
                for task in sub_task_responses:
                    if self._is_task_completed(task):
                        sub_task_responses.remove(task)
                if len(sub_task_responses) == 0:
                    waiting = False
                if i % 1000 == 0:
                    print("Still waiting on tasks.... {}".format(i))
                    i=0
                i = i + 1

            return self._compute_single_task(service_name, parameters)

    def _compute_single_task(self, service_name, parameters):
        data_description = self._build_data_description(service_name, parameters)
        print("Computing service " + data_description['output'])
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
                    'command': self._build_container_command(parameters)
                    # 'command': ["/bin/sh", "cd /app", 'ls', 'pwd', 'python3 --version', 'pip freeze']
                }
            ]}
        )
        print("Task running......")
        # print(json.dumps(task_response, indent=4, default=str))
        return task_response

    def _build_container_command(self, parameters):
        cmd = "/bin/sh -c \"cd /app && ls -altr && pwd && python3 --version && pip freeze "
        cmd = cmd + " && " + "python3 /app/main.py"
        cmd = cmd + "--parameters=\'{}\'".format(parameters)
        # for parameter in parameters.items():
        #     cmd = cmd + "{}: \'{}\',".format(parameter[0], parameter[1])
        # cmd = cmd + "}"
        cmd = cmd + "\""
        print("Container Parameters: {} Command: {}".format(parameters, cmd))
        return [cmd]

    def _build_data_description(self, service_name, parameters):
        ## TODO
         return {'name': service_name, 
                 'outputs': {'output1': "s3://" + self.BASE_BUCKET + "/" + service_name + "/"},
                 'inputs': parameters,
                 'parameters': parameters
         }