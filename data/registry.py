from pymongo import MongoClient


def add_service(service_description):
    client = MongoClient(port=27017)
    db = client.registry
    result = db.reviews.insert_one(service_description)
    print("Registered service description: " + result.inserted_id + " for service " + service_description["name"])
    return True
