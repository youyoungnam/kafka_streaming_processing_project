import json
from kafka import KafkaConsumer 
brokers =["localhost:9091", "localhost:9092", "localhost:9093"]

fraud_topic = "fraud_payments"
consumer = KafkaConsumer(fraud_topic, bootstrap_servers=brokers)



for messages in consumer:
    msg = json.loads(messages.value.decode())
    whoSend = msg["WHO"]
    amount = msg["AMOUNT"]
    if msg["WHO"] == "stranger":
        print(f"[ALTER] fraud detector payment {whoSend} -> {amount}")
    else:
        print(f"[Processing] payment {whoSend} -> {amount}")