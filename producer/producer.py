import json
import random
import time
import uuid

from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_SERVER = "localhost:9092"
TOPIC = "rides"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


cities = [
    "Casablanca",
    "Rabat",
    "Marrakech",
    "Tanger",
    "Agadir"
]


vehicle_types = [
    "standard",
    "premium",
    "van"
]


statuses = [
    "completed",
    "completed",
    "completed",
    "cancelled"
]


def generate_ride():

    distance = round(
        random.uniform(1.0, 30.0),
        2
    )

    fare = round(
        10 + distance * random.uniform(3.5, 5.5),
        2
    )

    ride = {

        "ride_id":
            str(uuid.uuid4()),

        "driver_id":
            f"DRV-{random.randint(1000, 9999)}",

        "customer_id":
            f"CUS-{random.randint(10000, 99999)}",

        "city":
            random.choice(cities),

        "vehicle_type":
            random.choice(vehicle_types),

        "distance_km":
            distance,

        "fare_mad":
            fare,

        "status":
            random.choice(statuses),

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat()

    }

    return ride


print("====================================")
print("   TAXI KAFKA PRODUCER STARTED")
print("====================================")


while True:

    ride = generate_ride()

    producer.send(
        TOPIC,
        value=ride
    )

    producer.flush()

    print(
        f"[SENT] "
        f"{ride['ride_id']} | "
        f"{ride['city']} | "
        f"{ride['fare_mad']} MAD"
    )

    time.sleep(1)