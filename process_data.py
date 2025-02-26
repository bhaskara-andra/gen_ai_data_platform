# Module 2: process_data.py (Kafka -> LLM)

import json
import os
from kafka import KafkaConsumer
# Import your LLM handling libraries here (e.g., OpenAI, Hugging Face Transformers)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "llm_consumer")

# LLM Configuration (replace with your actual setup)
# LLM_API_KEY = os.environ.get("LLM_API_KEY")
# LLM_MODEL_NAME = os.environ.get("LLM_MODEL_NAME")

def process_with_llm(data):
    # This is a placeholder for your LLM processing logic.
    # Replace with your actual LLM API calls and data processing.

    # Example: Simple analysis (replace with your LLM interaction)
    if data.get("average") is not None:
        if data["average"] > 80: # Example threshold
            print(f"Alert: High average metric value: {data['average']} at {data['timestamp']}")
            # Here you would send a prompt to the LLM, for example:
            # llm_response = call_llm(f"Analyze the following metric data: {data}. Generate a summary and identify potential issues.")
            # print(llm_response)
        else:
            print(f"Normal average metric value: {data['average']} at {data['timestamp']}")
    else:
        print("Metric data missing 'average' field.")

def consume_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        process_with_llm(data)

def main():
    consume_from_kafka()

if __name__ == "__main__":
    main()