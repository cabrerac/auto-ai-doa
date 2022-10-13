import json
from data.utils import make_hash
import time
import itertools

class Hypervisor():
    def __init__(self, base_bucket='climate-ensembling', cluster_name="HypervisorCluster", s3_client=None, ecs_client=None, registry=None):

        self.registry = registry
        self.s3_client =s3_client
        self.ecs_client = ecs_client
        self.BASE_BUCKET = base_bucket
        self.CLUSTER_NAME = cluster_name

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

    def pp_task_set(self, task_set):
        tshash = self.build_task_set_hash(task_set)
        print("\tTask set: len({}) type({}) TSHash({})".format(len(task_set), type(task_set), tshash))
        for j, task in enumerate(task_set):
            print("\t\tTask {} Hash {}: len({}) type({}) Service: {} \n\t\t\t{}".format(j, task['hash'], len(task), type(task), task['service'], task))
        print()


    def pretty_print_tasks_by_level(self, tasks_by_level):
        print("Tasks by Level Size:{}".format(len(tasks_by_level)))
        for key, value in tasks_by_level.items():
            print("\nLevel {} Length {}".format(key, len(value)))
            for i, task_set in enumerate(value):
                self.pp_task_set(task_set)

    def build_input_task_set_hashes(self, task_set):
        """
        Note this only works on a linear graph right now, if there are multiple inputs to a single node it will likely get confused and build hashes out of order.
        We'll need to add in a sense of lineage here for the sub-tasks to extend to multi-inputs. TODO tho :)

        :rtype: returns a dictionary of {"service": "task_set_hash"}
        """
        tshashes = {}
        ts = []
        for task in task_set:
            service = task['service']
            ts.append(task)
            tshashes[service] = {'tshash': self.build_task_set_hash(ts), 'task_set': ts.copy()}
        return tshashes



    def build_task_set_hash(self, task_set):
        """
        Makes a combined hash of the hashes of the sub-components for the overall task.
        :rtype: String of type "hash1/hash2/.../hashN"
        """
        combined_hash = ''
        for task in task_set:
            combined_hash += task['hash'] + '/'
        return combined_hash

    def hash_single_service(self, p):
        """
        Makes a hash out of an individual tasks parameters.

        """

        s2 = json.dumps(p, sort_keys=True)
        s3 = repr(s2)
        s4 = make_hash(s3)
        s5 = str(s4)
        return s5

    def _compute(self, service_name, parameters):
        print("Received request to compute for service: {} with parameters: {}\n".format(service_name, parameters))

        s_e = self.build_expanded_service_sets(parameters)

        preds = self.build_predecessor_level_dict(self.registry.service_graph, service_name)
        tasks_by_level = self.build_tasks_per_level(preds, s_e)
        self.pretty_print_tasks_by_level(tasks_by_level)

        self.compute_tasks_by_level(tasks_by_level)

    def build_expanded_service_sets(self, parameters):
        """
        Returns, for each individual service, the power expansion of the parameters for the service.

        <-- {"SelectLocation": [(a,1), (a,2), (b,1), (b,2)], ...}
        """
        expanded_service_sets = {}
        for service, sp in parameters.items():
            combos = list(itertools.product(*sp.values()))
            keyed_combos = tuple(map(lambda values: dict(zip(sp.keys(), values)), combos))
            expanded_service_sets[service] = keyed_combos

        s_e_hash = {}

        for service, service_tasks in expanded_service_sets.items():
            tasks_hashed = []
            for task in service_tasks:
                h = self.hash_single_service(task)
                tasks_hashed.append({"service": service, "hash": h, "task": task})
            s_e_hash[service] = tasks_hashed

        return s_e_hash

    def build_predecessor_level_dict(self, graph, source):
        """
        Traverses the graph and returns a dictionary of each level of the graph,
            starting from the source node and working its way up along predecessors.
        """
        level = 0
        predecessors = {}
        done = False
        currentLevel = set([source])
        nextLevel = set()


        # TODO This isn't perfect, as it won't do secondary chains that are shorter before we reach their level. But that's okay.
        while not done:
            predecessors[level] = currentLevel
            level += 1
            for current in currentLevel:
                for p in graph.predecessors(current):
                    nextLevel.add(p)
            if len(nextLevel) == 0:
                done = True
            currentLevel = nextLevel
            nextLevel = set()
        reversed_predecessors = {}
        for k, v in predecessors.items():
            reversed_predecessors[level-1-k] = v
        return reversed_predecessors

    def build_tasks_per_level(self, preds, s_e):
        """
        
        :rtype: TODO please :) should be a more consistent thing.
        """
        
        start_level = 0
        level = start_level
        done = False
        tasks_by_level = {}
        while not done:
            level_services = preds[level]
            tasks_by_level[level] = []
            for service in level_services:
                task_set = s_e[service]
                for new_task in task_set:
                    if level > 0:
                        for previous_tasks in tasks_by_level[level-1]:
                            pt_copy = previous_tasks.copy()
                            pt_copy.append(new_task)
                            tasks_by_level[level].append(pt_copy)
                    else:
                        tasks_by_level[level].append([new_task])

            level += 1
            if level not in preds:
                done=True

        return tasks_by_level


    def compute_tasks_by_level(self, tasks_by_level):
        for level, level_tasks in tasks_by_level.items():
            print("Level: {}".format(level))
            if level >= 0:
                for task_set in level_tasks:
                    self.compute_task(task_set)

    def pp_tshashes(self, tshashes, service=None):
        for service, tshash in tshashes.items():
            hash = tshash['tshash']
            task_set = tshash['task_set']

    def compute_task(self, task_set):
        service_name = task_set[-1]['service']
        tshashes = self.build_input_task_set_hashes(task_set)
        self.pp_tshashes(tshashes, service_name)

        input_hashes = {}
        for service, tshash in tshashes.items():
            if service != service_name:
                input_hashes[service] = tshash['tshash']

        output_hash = tshashes[service_name]['tshash']
        parameters = {service_name: task_set[-1]['task']}

        # just need to pass the list of hashes here when building the description.
        task_description = self.registry.build_data_description_from_task(service_name, input_hashes, output_hash, parameters)
        
        print(task_description)
        print("\n\n")

        self._compute_single_service(task_description)

    def _compute_single_service(self, task_command_description):

        # Call docker
        taskDefinition = "ClimateEnsemblingManual"

        print("Calling task {} with inputs from {} and output to: {}".format(task_command_description['dataId'], task_command_description['inputs'], task_command_description['outputs']))
        # print("**** DEBUG **** Calling task {} with data description: {}".format(data_description))

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
                    'command': self._build_container_command(task_command_description)
                }
            ]}
        )
        print("ECS Task {} running......".format(task_command_description['dataId']))
        return task_response

    def _build_container_command(self, parameters):
        cmd = "/bin/sh -c \"cd /app && ls -altr && pwd && python3 --version && pip freeze "
        cmd = cmd + " && " + "python3 /app/main.py "
        cmd = cmd + "--parameters='{}'".format(json.dumps(parameters)).replace("\"", "\\\"")
        cmd = cmd + "\""
        # print("\nContainer Parameters: {} \n\n Command: {}".format(parameters, cmd))
        return [cmd]

    def _is_service_completed(self, target_task_arn):
        taskList = self.ecs_client.list_tasks(cluster=self.CLUSTER_NAME)['taskArns'] #desiredStatus='STOPPED')
        describedTasks = self.ecs_client.describe_tasks(cluster=self.CLUSTER_NAME, tasks=taskList)
        for task_arn, task_response in zip(taskList, describedTasks):
            if task_arn == target_task_arn:
                print("Target task located in list.")
        return False


    # def _compute_old(self, service_name, parameters):
    #     ancestors = self.registry.get_ancestors_and_self(service_name)
    #     subset_parameters = get_required_parameters(ancestors, parameters, self.registry.service_graph)
    #     dataHash = self.registry.hash_single_service(service_name, subset_parameters)
    #     dataId = self.registry.make_data_id(service_name, dataHash)
    #     cached_data = self.registry.get_data(dataId)

    #     print("***** Inside _compute for {} with dataHash {} *****".format(service_name, dataHash))

    #     if not parameters['force_rerun'] and (cached_data is not None):
    #     # TODO if cached_data is not None or service_name in inputs:
    #         print("Returning cached data for {}: {}".format(service_name, cached_data))
    #         return cached_data
    #     else:
    #         waiting = True
    #         service = self.registry.get_service(service_name, subset_parameters)
    #         sub_service_responses = []

    #         # Dispatch jobs or get the cached data for the required inputs
    #         for input in service['inputs']:
    #             sub_service_responses.append(self._compute(input, parameters))
    #         print("Service {} Subservice Reponses {}".format(service_name, len(sub_service_responses)))
            
    #         # Sync - Wait for all the services to complete before doing the upper service
    #         i=0
    #         while waiting:
    #             # TODO Make this better parallelized for computing all the models at once
    #             for s in sub_service_responses:
    #                 if self._is_service_completed(s):
    #                     sub_service_responses.remove(s)
    #             if len(sub_service_responses) == 0:
    #                 waiting = False
    #                 continue
    #             if i % 10 == 0:
    #                 print("Still waiting on services.... {}".format(i))
    #                 i=0
    #                 # TODO remove
    #                 waiting=False
    #             i = i + 1
    #             time.sleep(3) # sleep for 15 seconds in between checks


    #         return self._compute_single_service_multi_parameters(service_name, service, parameters)

    # def _split_parameters_into_individual_task_sets(self, service_name, parameters):
    #     """
    #     Takes a list of all the parameter combinations for this task, and splits it into separate tasks to run each one.s
        
    #     Approximately the below, but wrapped with the service name and other HV parameters.
    #     Input parameters = {'a': [1,2], 'b': [3,3,4], 'c': [9]}
    #     => Output ~ [{'a': 1, 'b': 3, 'c': 9}, {'a': 1, 'b': 3, 'c': 9}, {'a': 1, 'b': 4, 'c': 9}, {'a': 2, 'b': 3, 'c': 9}, {'a': 2, 'b': 3, 'c': 9}, {'a': 2, 'b': 4, 'c': 9}]
    #     """
    #     parameter_sets = []
    #     relevant_services = [service_name]
    #     service_params = parameters[service_name]
    #     traverse_name = service_name
    #     ancestors = self.registry.get_ancestors_and_self(traverse_name)

    #     # First we loop through the ancestors and build an overall parameters set for all previous services of the requested service
    #     for ancestor in ancestors:
    #         print("Ancestor: {}".format(ancestor))
    #         if ancestor not in relevant_services:
    #             relevant_services.append(ancestor)
    #             service_params = {**service_params, **parameters[ancestor]}
                    

    #     # TODO maybe keep the dictionary-ness here rather than dropping it then turning it back into a dict.

    #     # Here we expand the lists inside the parameters and build the combintoric combinations
    #     combos = list(itertools.product(*service_params.values()))


    #     # print("*****DEBUG COMBOS {}".format(combos))

    #     # Add back in the keys for the 
    #     keyed_combos = []
    #     for combo in combos:
    #         keyed_combos.append(dict(zip(service_params.keys(), combo)))


    #     for kc in keyed_combos:
    #         pcopy = parameters.copy()
    #         pcopy[service_name] = kc
    #         parameter_sets.append(pcopy)
    #     print("Initial Parameters: {} \n Output parameters sets: {} \n".format(service_name, parameters, len(parameter_sets)))
    #     # print("**** DEBUG Output parameters sets: {} \n\n\n\n".format(parameter_sets))
    #     return parameter_sets

    # def _compute_single_service_multi_parameters(self, service_name, service, parameters):
    #     parameter_sets = self._split_parameters_into_individual_task_sets(service_name, parameters)
    #     for single_set_parameters in parameter_sets:
    #         self._compute_single_service(service_name, service, single_set_parameters)

