from flask import (Flask, request, make_response, jsonify)

from modules.hypervisor import Hypervisor
import boto3
from data.registry import DynamoDBRegistry

app = Flask(__name__)

ddb_client = boto3.resource("dynamodb")
registry = DynamoDBRegistry(ddb_client)
s3_client = boto3.client("s3")
ecs_client = boto3.client("ecs")
hypervisor = Hypervisor(registry=registry, s3_client=s3_client, ecs_client=ecs_client)


@app.route("/auto_ai/hypervisor/register_service", methods=["GET", "POST", "PUT", "DELETE"])
def register_service():
    res = {}
    service_description = request.get_json()
    if hypervisor.register_service(service_description):
        res["response"] = "Service registered."
    else:
        res["response"] = "Service not registered."
    res = make_response(jsonify(res), 200)
    return res


@app.route("/auto_ai/hypervisor/execute_service", methods=["GET", "POST", "PUT", "DELETE"])
def execute_service():
    res = {}
    service_request = request.get_json()
    if hypervisor.execute_service(service_request):
        res["response"] = "Executing service."
    else:
        res["response"] = "Service not executed."
    res = make_response(jsonify(res), 200)
    return res

if __name__ == "__main__":
    app.run(host="127.0.0.1:5000")
