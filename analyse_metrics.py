# metrics_analyzer.py
import google.generativeai as genai
import os
import json
from kafka import KafkaConsumer
import streamlit as st

# Gemini API Configuration
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))
model = genai.GenerativeModel('gemini-pro')

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "llm_metrics_consumer")

def analyze_metrics(metrics_data):
    prompt = f"""
    Analyze the following pipeline failure metrics:

    {json.dumps(metrics_data, indent=2)}

    Identify:
    - Any recurring patterns in failures.
    - The most frequent failure causes.
    - The time periods with the highest failure rates.
    - Potential root causes of the failures.
    - Summarize the analysis.
    """
    response = model.generate_content(prompt)
    return response.text

def consume_metrics():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    return consumer

def main():
    st.title("Pipeline Metrics Analysis Dashboard")
    consumer = consume_metrics()
    for message in consumer:
        metrics_data = message.value
        analysis = analyze_metrics(metrics_data)
        st.write("## Analysis Results:")
        st.write(analysis)
        st.write("## Raw Metrics Data:")
        st.json(metrics_data)

if __name__ == "__main__":
    main()