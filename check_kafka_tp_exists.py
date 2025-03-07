from kafka import KafkaAdminClient

def check_if_topic_exists(topic_name,bootstrap_server):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_server)
    topics = admin_client.list_topics()
    if topic_name in topics:
        print(f"Topic '{topic_name}' exists")
    else:
        print(f"Topic '{topic_name}' does not exist")