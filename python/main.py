import os
import time
import uuid
from confluent_kafka import SerializingProducer  # Kafka producer for sending messages
import simplejson as json  # JSON serialization with support for decimals
from datetime import datetime, timedelta
import random

# ----------------------------------------
# Constants for geographic simulation
# ----------------------------------------
LONODN_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}  # London coordinates
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}  # Birmingham coordinates

# Simulate vehicle movement increments for 100 messages
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"] - LONODN_COORDINATES["latitude"]) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["longitude"] - LONODN_COORDINATES["longitude"]) / 100

# ----------------------------------------
# Environment variables for configuration
# ----------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", 'vehicle_data')
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

# Seed random number generator for reproducibility
random.seed(42)

# Initialize simulation start time and location
start_time = datetime.now()
start_location = LONODN_COORDINATES.copy()

# ----------------------------------------
# Helper functions for generating timestamps
# ----------------------------------------
def get_next_time():
    """
    Simulates the next timestamp for generated messages.
    Increments current time by a random 30-60 seconds.
    """
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))  # Update frequency
    return start_time

# ----------------------------------------
# Functions for generating Kafka messages
# ----------------------------------------
def generate_gps_data(device_id, timestamp, vehicle_type="private"):
    """
    Generate simulated GPS data.
    """
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "speed": random.uniform(0, 40),  # km/h
        "direction": "North-East",
        "vehicleType": vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    """
    Generate simulated traffic camera data.
    """
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "cameraId": camera_id,
        "location": location,
        "timestamp": timestamp,
        "snapshot": "Base64EncodedString"  # Placeholder, no real image
    }

def generate_weather_data(device_id, timestamp, location):
    """
    Generate simulated weather data.
    """
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "location": location,
        "timestamp": timestamp,
        "temperature": random.uniform(-5, 28),
        "weatherCondition": random.choice(["Sunny", "Cloudy", "Rain", "Snow"]),
        "precipitation": random.uniform(0, 25),
        "windSpeed": random.uniform(0, 100),
        "humidity": random.randint(0, 100),  # percentage
        "airQualityIndex": random.uniform(0, 500)
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    """
    Generate simulated emergency incident data.
    """
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "incidentId": uuid.uuid4(),
        "type": random.choice(["Accident", "Fire", "Medical", " Police", "None"]),
        "timestamp": timestamp,
        "location": location,
        "status": random.choice(["Active", "Resolved"]),
        "description": "Description of the incident"
    }

# ----------------------------------------
# Vehicle movement simulation
# ----------------------------------------
def simulate_vehicle_movement():
    """
    Simulate the vehicle moving from London to Birmingham.
    Adds a small random delta to simulate realistic movement.
    """
    global start_location

    # Move towards Birmingham
    start_location["latitude"] += LATITUDE_INCREMENT
    start_location["longitude"] += LONGITUDE_INCREMENT

    # Add some randomness to simulate actual road travel
    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(device_id):
    """
    Generate vehicle data including current location, speed, and metadata.
    """
    location = simulate_vehicle_movement()
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": get_next_time().isoformat(),
        "location": (location["latitude"], location["longitude"]),
        "speed": random.uniform(10, 40),
        "direction": "North-East",
        "make": "BMW",
        "model": "M340",
        "Year": 2024,
        "fuelType": "Hybrid"
    }

# ----------------------------------------
# Kafka helpers
# ----------------------------------------
def json_serializer(obj):
    """
    Serialize objects for JSON; converts UUIDs to strings.
    """
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def delivery_report(err, msg):
    """
    Callback for Kafka message delivery reports.
    """
    if err is not None:
        print(f"Message delivery failed {err}")
    else:
        print(f"Message delivered to {msg.topic()} on partition [{msg.partition()}]")

def produce_data_to_kafka(producer, topic, data):
    """
    Produce a single message to Kafka topic.
    """
    producer.produce(
        topic,
        key=str(data["id"]),  # Message key
        value=json.dumps(data, default=json_serializer).encode("utf-8"),  # Message value
        on_delivery=delivery_report
    )
    producer.flush()  # Ensure message is sent immediately

# ----------------------------------------
# Main simulation loop
# ----------------------------------------
def simulate_journey(producer, device_id):
    """
    Simulate the journey of a single vehicle.
    Generates and sends data for vehicle, GPS, traffic, weather, and emergency.
    Stops when vehicle reaches Birmingham.
    """
    while True:
        # Generate data for all topics
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data["timestamp"])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data["timestamp"], vehicle_data["location"], camera_id="Nikon")
        weather_data = generate_weather_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])

        # Stop simulation if vehicle reaches Birmingham
        if (vehicle_data["location"][0] >= BIRMINGHAM_COORDINATES["latitude"]) and (vehicle_data["location"][1] <= BIRMINGHAM_COORDINATES["longitude"]):
            print("Vehicle has reached Birmingham. Simulation ending...")
            break

        # Send data to Kafka topics
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        # Separator in console for readability
        print("****************************************************")
        print("****************************************************")
        print("****************************************************")

        time.sleep(5)  # Wait 5 seconds before generating next batch

# ----------------------------------------
# Entry point
# ----------------------------------------
if __name__ == "__main__":
    # Kafka producer configuration
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "error_cb": lambda err: print(f"Kafka error:{err}")  # Callback for errors
    }

    # Initialize producer
    producer = SerializingProducer(producer_config)
    try:
        # Start simulation for a specific vehicle
        simulate_journey(producer, 'Vehicle-Honda003')

    except KeyboardInterrupt:
        print("Simulation ended by the user")

    except Exception as e:
        print(f"Unexpected error occured: {e}")
