from pymongo import MongoClient


def add_service(service_description):
    client = MongoClient(port=27017)
    db = client.registry
    result = db.registry.insert_one(service_description)
    print("Registered service description: " + str(result.inserted_id) + " for service " + service_description["name"])
    return True
