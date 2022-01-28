import json 
from ensurepip import bootstrap
from kafka import KafkaConsumer 


brokers =["localhost:9091", "localhost:9092", "localhost:9093"]
legit_topic = "legit_topic"
consumer = KafkaConsumer(legit_topic, bootstrap_servers=brokers)



for messages in consumer:
    msg = json.loads(messages.value.decode())
    to = msg["WHO"]
    amount = msg["AMOUNT"]
    if msg["PAYMENTTYPE"] == "VISA":
        print(f"[VIAS] payment {to} - {amount}")
    elif msg["PAYMENTTYPE"] == "MASTERCARD":
        print(f"[MASTERCARD] payment {to} - {amount}")
    else:
        print("[ALTER] unable to process payments") 