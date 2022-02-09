from data import registry


def register_service(service_description):
    return registry.add_service(service_description)


def execute_service(service_request):
    service_name = service_request['name']
    service_parameters = service_request['parameters']
    cached_data = registry.get(service_name, service_parameters)
    if cached_data:
        return cached_data
    else:
        _compute(service_name, service_parameters)


def _compute(service_name, service_parameters):
    print("Computing service " + service_name)
    # Call docker
    # Fill data in registry


