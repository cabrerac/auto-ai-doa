import json
from data.utils import get_required_parameters
import time
import itertools

class Hypervisor():
    def __init__(self, base_bucket='climate-ensembling', cluster_name="HypervisorCluster", s3_client=None, ecs_client=None, registry=None):

        self.registry = registry
        self.s3_client =s3_client
        self.ecs_client = ecs_client
        self.BASE_BUCKET = base_bucket
        self.CLUSTER_NAME = cluster_name

        # response = self.ecs_client.create_cluster(clusterName=self.CLUSTER_NAME)

    def register_service(self, service_description):
        print("Register: Service description {}".format(service_description))
        registry_response = self.registry.put_service(service_description)

        return registry_response

    def execute_service(self, service_request):
        print("Execute: Service description {} \n \n ".format(service_request))
        service_name = service_request['target_service']
        service_parameters = service_request['parameters']
        service_response = self._compute(service_name, service_parameters)
        return service_response

    def _compute(self, service_name, parameters):
        ancestors = self.registry.get_ancestors_and_self(service_name)
        subset_parameters = get_required_parameters(ancestors, parameters, self.registry.service_graph)
        dataHash = self.registry.data_item_hash(service_name, subset_parameters)
        dataId = self.registry.make_data_id(service_name, dataHash)
        cached_data = self.registry.get_data(dataId)

        print("***** Inside _compute for {} with dataHash {} *****".format(service_name, dataHash))

        if not parameters['force_rerun'] and (cached_data is not None):
        # TODO if cached_data is not None or service_name in inputs:
            print("Returning cached data for {}: {}".format(service_name, cached_data))
            return cached_data
        else:
            waiting = True
            service = self.registry.get_service(service_name, subset_parameters)
            sub_service_responses = []

            # Dispatch jobs or get the cached data for the required inputs
            for input in service['inputs']:
                sub_service_responses.append(self._compute(input, parameters))
            print("Service {} Subservice Reponses {}".format(service_name, len(sub_service_responses)))
            
            # Sync - Wait for all the services to complete before doing the upper service
            i=0
            while waiting:
                # TODO Make this better parallelized for computing all the models at once
                for s in sub_service_responses:
                    if self._is_service_completed(s):
                        sub_service_responses.remove(s)
                if len(sub_service_responses) == 0:
                    waiting = False
                    continue
                if i % 10 == 0:
                    print("Still waiting on services.... {}".format(i))
                    i=0
                    # TODO remove
                    waiting=False
                i = i + 1
                time.sleep(3) # sleep for 15 seconds in between checks


            return self._compute_single_service_multi_parameters(service_name, service, parameters)

    def _is_service_completed(self, target_task_arn):
        taskList = self.ecs_client.list_tasks(cluster=self.CLUSTER_NAME)['taskArns'] #desiredStatus='STOPPED')
        describedTasks = self.ecs_client.describe_tasks(cluster=self.CLUSTER_NAME, tasks=taskList)
        for task_arn, task_response in zip(taskList, describedTasks):
            if task_arn == target_task_arn:
                print("Target task located in list.")
        return False

    def _split_parameters_into_individual_task_sets(self, service_name, parameters):
        """
        Takes a list of all the parameter combinations for this task, and splits it into separate tasks to run each one.s
        
        Approximately the below, but wrapped with the service name and other HV parameters.
        Input parameters = {'a': [1,2], 'b': [3,3,4], 'c': [9]}
        => Output ~ [{'a': 1, 'b': 3, 'c': 9}, {'a': 1, 'b': 3, 'c': 9}, {'a': 1, 'b': 4, 'c': 9}, {'a': 2, 'b': 3, 'c': 9}, {'a': 2, 'b': 3, 'c': 9}, {'a': 2, 'b': 4, 'c': 9}]
        """
        parameter_sets = []
        relevant_services = [service_name]
        service_params = parameters[service_name]
        traverse_name = service_name
        ancestors = self.registry.get_ancestors_and_self(traverse_name)

        # First we loop through the ancestors and build an overall parameters set for all previous services of the requested service
        for ancestor in ancestors:
            print("Ancestor: {}".format(ancestor))
            if ancestor not in relevant_services:
                relevant_services.append(ancestor)
                service_params = {**service_params, **parameters[ancestor]}
                    

        # TODO maybe keep the dictionary-ness here rather than dropping it then turning it back into a dict.

        # Here we expand the lists inside the parameters and build the combintoric combinations
        combos = list(itertools.product(*service_params.values()))


        # print("*****DEBUG COMBOS {}".format(combos))

        # Add back in the keys for the 
        keyed_combos = []
        for combo in combos:
            keyed_combos.append(dict(zip(service_params.keys(), combo)))


        for kc in keyed_combos:
            pcopy = parameters.copy()
            pcopy[service_name] = kc
            parameter_sets.append(pcopy)
        print("Initial Parameters: {} \n Output parameters sets: {} \n".format(service_name, parameters, len(parameter_sets)))
        # print("**** DEBUG Output parameters sets: {} \n\n\n\n".format(parameter_sets))
        return parameter_sets

    def _compute_single_service_multi_parameters(self, service_name, service, parameters):
        parameter_sets = self._split_parameters_into_individual_task_sets(service_name, parameters)
        for single_set_parameters in parameter_sets:
            self._compute_single_service(service_name, service, single_set_parameters)

    def _compute_single_service(self, service_name, service, parameters):
        
        data_hash = self.registry.data_item_hash(service_name, parameters)
        data_description = self.registry.build_data_description(service_name, service, data_hash, parameters, s3_bucket='climate-ensembling')
        # Fill data location into registry
        if not self.registry.put_data(data_description):
            print("Data not registered successfully! {}".format(data_description))

        # Call docker
        taskDefinition = "ClimateEnsemblingManual"

        print("Calling task {} with inputs from {} and output to: {}".format(data_description['dataId'], data_description['inputs'], data_description['outputs']))
        # print("**** DEBUG **** Calling task {} with data description: {}".format(data_description))

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
        print("ECS Task {} running......".format(data_description['dataId']))
        return task_response

    def _build_container_command(self, parameters):
        cmd = "/bin/sh -c \"cd /app && ls -altr && pwd && python3 --version && pip freeze "
        cmd = cmd + " && " + "python3 /app/main.py "
        cmd = cmd + "--parameters='{}'".format(json.dumps(parameters)).replace("\"", "\\\"")
        cmd = cmd + "\""
        # print("\nContainer Parameters: {} \n\n Command: {}".format(parameters, cmd))
        return [cmd]
