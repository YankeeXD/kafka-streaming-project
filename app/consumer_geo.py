from kafka import KafkaConsumer, KafkaProducer
import requests
import json
import time
import re
from cachetools import TTLCache

# Kafka topics
input_topic = 'user-login'
output_topic = 'processed-login'

# Configure Kafka producer and consumer
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    group_id='consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# API configuration
API_URL = "http://ip-api.com/json"
cache = TTLCache(maxsize=1000, ttl=3600)  # Cache results for 1 hour
request_counter = 0
MAX_REQUESTS_PER_MINUTE = 40  # Safe limit below 45 requests per minute

def is_valid_ip(ip):
    """Validate if the IP is public and not reserved/private."""
    private_ip_patterns = [
        re.compile(r"^10\."),  # Private IP range 10.0.0.0/8
        re.compile(r"^172\.(1[6-9]|2[0-9]|3[0-1])\."),  # Private IP range 172.16.0.0/12
        re.compile(r"^192\.168\."),  # Private IP range 192.168.0.0/16
        re.compile(r"^127\."),  # Loopback IP
        re.compile(r"^100\."),  # Reserved IP range
        re.compile(r"^224\."),  # Multicast IP range
        re.compile(r"^240\."),  # Reserved for future use
    ]
    return not any(pattern.match(ip) for pattern in private_ip_patterns)

def get_location(ip_address):
    """Fetch geolocation for an IP address."""
    global request_counter

    if ip_address in cache:
        return cache[ip_address]
    if not is_valid_ip(ip_address):
        return {"country": "Unknown", "region": "Unknown"}

    if request_counter >= MAX_REQUESTS_PER_MINUTE:
        print("Rate limit reached. Sleeping for 60 seconds...")
        time.sleep(60)  # Pause to avoid hitting the rate limit
        request_counter = 0  # Reset counter

    try:
        response = requests.get(f"{API_URL}/{ip_address}")
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                location = {
                    "country": data.get("country", "Unknown"),
                    "region": data.get("regionName", "Unknown"),
                }
                cache[ip_address] = location
                request_counter += 1
                return location
        print(f"API Error: {response.status_code}, {response.content}")
        return {"country": "Unknown", "region": "Unknown"}
    except Exception as e:
        print(f"Error fetching location for IP {ip_address}: {e}")
        return {"country": "Unknown", "region": "Unknown"}

# Processing Kafka messages
aggregated_data = {}

for message in consumer:
    try:
        # Read message
        data = message.value

        # Handle missing fields
        if 'ip' not in data or 'timestamp' not in data:
            continue

        # Get location
        location_info = get_location(data['ip'])
        location = f"{location_info['country']}-{location_info['region']}"

        # Aggregate data
        if location not in aggregated_data:
            aggregated_data[location] = {"total_users": 0, "timestamp_sum": 0}

        aggregated_data[location]["total_users"] += 1
        aggregated_data[location]["timestamp_sum"] += float(data['timestamp'])
        avg_timestamp = aggregated_data[location]["timestamp_sum"] / aggregated_data[location]["total_users"]

        # Prepare data for output
        processed_data = {
            "location": location,
            "total_users": aggregated_data[location]["total_users"],
            "avg_timestamp": avg_timestamp,
        }

        # Send to output topic
        producer.send(output_topic, value=processed_data)

    except Exception as e:
        print(f"Error processing message: {e}")
