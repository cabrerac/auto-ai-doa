from flask import (Flask, request, make_response, jsonify)

from modules import hypervisor

app = Flask(__name__)


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
