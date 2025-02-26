# Module 1: data_ingestion.py (Azure Metrics -> Kafka)

import json
import os
from azure.monitor import MonitorClient
from azure.common.credentials import ServicePrincipalCredentials
from kafka import KafkaProducer

# Azure Configuration
TENANT_ID = os.environ.get("AZURE_TENANT_ID")
CLIENT_ID = os.environ.get("AZURE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET")
SUBSCRIPTION_ID = os.environ.get("AZURE_SUBSCRIPTION_ID")
RESOURCE_ID = os.environ.get("AZURE_RESOURCE_ID")  # e.g., /subscriptions/{sub_id}/resourceGroups/{rg}/providers/{provider}/{resourceType}/{resourceName}
METRIC_NAMESPACE = os.environ.get("AZURE_METRIC_NAMESPACE")
METRIC_NAME = os.environ.get("AZURE_METRIC_NAME")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")

def get_azure_metrics():
    credentials = ServicePrincipalCredentials(
        client_id=CLIENT_ID,
        secret=CLIENT_SECRET,
        tenant=TENANT_ID
    )
    monitor_client = MonitorClient(credentials, SUBSCRIPTION_ID)

    metrics = monitor_client.metrics.list(
        resource_uri=RESOURCE_ID,
        metricnamespace=METRIC_NAMESPACE,
        metricnames=[METRIC_NAME]
    )

    metric_data = []

    for item in metrics.value:
        for timeseries in item.timeseries:
            for data in timeseries.data:
                metric_data.append({
                    "timestamp": str(data.timestamp),
                    "average": data.average,
                    "minimum": data.minimum,
                    "maximum": data.maximum,
                    "total": data.total,
                    "count": data.count
                })

    return metric_data

def send_to_kafka(data):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for record in data:
        producer.send(KAFKA_TOPIC, value=record)

    producer.flush()
    producer.close()

def main():
    metrics = get_azure_metrics()
    if metrics:
        send_to_kafka(metrics)
        print("Azure metrics sent to Kafka.")
    else:
        print("No metrics retrieved.")

if __name__ == "__main__":
    main()