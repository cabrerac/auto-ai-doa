
def get_required_parameters(ancestors, parameters):
    req_parameters = {}
    for a in ancestors:
        for p in a.parameters.keys():
            if p in parameters:
                req_parameters[p] = parameters[p]
            else:
                print("Required parameter {} is not in parameters list!!!!".format(p))
    return req_parameters
