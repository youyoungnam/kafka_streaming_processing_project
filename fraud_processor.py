import os 
import ast
import json
from kafka import KafkaConsumer 
from db import Config
from dotenv import load_dotenv
from os.path import join, dirname 

dotenv_path = join(dirname(__file__), './.env')
load_dotenv(dotenv_path)
brokers = ast.literal_eval(os.environ.get("BROKERS"))

fraud_topic = os.environ.get("FRAUD_TOPIC")
consumer = KafkaConsumer(fraud_topic, bootstrap_servers=brokers)

insertQuery = Config()

def push_sql(querys):
    insertQuery.execute(querys)

for messages in consumer:
    msg = json.loads(messages.value.decode())
    whoSend = msg["WHO"]
    amount = msg["AMOUNT"]
    paymentType = msg["PAYMENTTYPE"]
    date = msg["DATE"]
    time = msg["TIME"]
    if msg["WHO"] == "STRANGER":
        print(f"[ALTER] fraud detector payment {whoSend} -> {amount}")
        querys = f"""    
        INSERT fraud_detector(
             DATE, TIME, PAYMENTTYPE, AMOUNT, WHO
        ) VALUES ("{date}", "{time}", "{paymentType}", {amount}, "{whoSend}"
            )        
        """
        push_sql(querys)