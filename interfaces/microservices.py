from flask import (Flask, request, make_response, jsonify)

from modules import hypervisor

app = Flask(__name__)


@app.route("/auto_ai/hypervisor/register_service", methods=["GET", "POST", "PUT", "DELETE"])
def register_service():
    res = {}
    service_description = request.get_json()
    if hypervisor.registry(service_description):
        res["response"] = "Service registered."
    else:
        res["response"] = "Service not registered."
    res = make_response(jsonify(res), 200)
    return res


app.run(host="127.0.0.1:5000")
