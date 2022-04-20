import hashlib

HASH_DIGEST_SIZE = 8
def get_required_parameters(ancestors, parameters, graph):
    print("\n*************Get Required Parameters *********************")
    print("Ancestors {} \n\n Parameters {} \n".format(ancestors, parameters))
    req_parameters = {}
    for a in ancestors:
        req_parameters[a] = {}
        anode = graph.nodes[a]
        # print("Node p {} {}".format(a, graph.nodes[a]))
        for p in anode['parameters'].keys():
            # print("Parameter p {} Parameters {}".format(p, parameters[a].keys()))
            if p in parameters[a]:
                req_parameters[a][p] = parameters[a][p]
            else:
                print("Required parameter {} is not in parameters list!!!!".format(p))
    print("Returning required parameters {}".format(req_parameters))
    return req_parameters

def make_hash(s):
    return hashlib.blake2s(str.encode(s), digest_size=HASH_DIGEST_SIZE).hexdigest()
    # return hashlib.sha224(b"Nobody inspects the spammish repetition").hexdigest()
    # m = hashlib.sha256()
    # m.update(s)
    # return m.digest()
