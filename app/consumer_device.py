import json
from kafka import KafkaConsumer, KafkaProducer

# Kafka topics
input_topic = 'user-login'
output_topic = 'processed-login'

# Kafka consumer and producer
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    group_id='device-type-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Aggregation dictionary
device_type_data = {}

# Process messages
for message in consumer:
    try:
        # Read message
        data = message.value

        # Handle missing fields
        if 'device_type' not in data or 'timestamp' not in data:
            continue

        device_type = data['device_type']
        timestamp = float(data['timestamp'])

        # Aggregate data by device_type
        if device_type not in device_type_data:
            device_type_data[device_type] = {'total_users': 0, 'timestamp_sum': 0.0}

        device_type_data[device_type]['total_users'] += 1
        device_type_data[device_type]['timestamp_sum'] += timestamp

        # Calculate average timestamp and filter
        for dtype, stats in device_type_data.items():
            if stats['total_users'] >= 100:  # Filter condition
                avg_timestamp = stats['timestamp_sum'] / stats['total_users']

                # Prepare aggregated data
                aggregated_data = {
                    'device_type': dtype,
                    'total_users': stats['total_users'],
                    'avg_timestamp': avg_timestamp
                }

                # Send to output topic
                producer.send(output_topic, value=aggregated_data)

    except Exception as e:
        print(f"Error processing message: {e}")
