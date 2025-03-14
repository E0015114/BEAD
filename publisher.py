import requests
import json
from google.cloud import pubsub_v1

# Your variables
project_id = "qwiklabs-gcp-00-f8b9a2acad93"
topic_id = "lta-carpark-availability"
lta_api_url = "https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2"
lta_api_key = "7GQ4fcMqRTuEm4Tb681Y6A=="

# Set up Pub/Sub client (ensure the service account JSON file is provided)
publisher = pubsub_v1.PublisherClient.from_service_account_file('path_to_your_service_account.json')
topic_path = publisher.topic_path(project_id, topic_id)

# Fetch data from LTA API
headers = {"AccountKey": lta_api_key}
response = requests.get(lta_api_url, headers=headers)

if response.status_code == 200:
    data = response.json()
    message = json.dumps(data)

    # Publish message to Pub/Sub
    future = publisher.publish(topic_path, data=message.encode("utf-8"))
    print(f"Published message ID: {future.result()}")
else:
    print(f"Failed to fetch data: {response.status_code} - {response.text}")
