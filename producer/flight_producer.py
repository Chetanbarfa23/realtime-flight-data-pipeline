import requests
import json
import time
from kafka import KafkaProducer

# Connect Python to Kafka
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

url = "https://opensky-network.org/api/states/all"

while True:

    response = requests.get(url)
    data = response.json()

    states = data.get("states", [])

    print("Aircraft received:", len(states))

    for flight in states[:10]:

        flight_data = {
            "icao24": flight[0],
            "callsign": flight[1].strip() if flight[1] else None,
            "country": flight[2],
            "longitude": flight[5],
            "latitude": flight[6],
            "altitude": flight[7],
            "velocity": flight[9]
        }

        # Send flight event to Kafka
        producer.send(
                "flights",
                key=flight_data["icao24"].encode("utf-8"),
                value=flight_data
        )

        print("Sent:", flight_data)

    producer.flush()

    print("Waiting 30 seconds...\n")

    time.sleep(30)


