import os
import json 
import ast
from ensurepip import bootstrap
from kafka import KafkaConsumer 
from db import Config
from dotenv import load_dotenv
from os.path import join, dirname 

dotenv_path = join(dirname(__file__), './.env')
load_dotenv(dotenv_path)
brokers = ast.literal_eval(os.environ.get("BROKERS"))
legit_topic = os.environ.get("LEGIT_TOPIC")

consumer = KafkaConsumer(legit_topic, bootstrap_servers=brokers)
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
    if msg["PAYMENTTYPE"] == "VISA" or msg["PAYMENTTYPE"] == "MASTERCARD":
        print(f"[VIAS] payment {whoSend} - {amount}")
        querys = f"""    
        INSERT legit_detector(
             DATE, TIME, PAYMENTTYPE, AMOUNT, WHO
        ) VALUES ("{date}", "{time}", "{paymentType}", {amount}, "{whoSend}"
            )        
        """
        push_sql(querys)
    else:
        print(f"[ALTER] STRANGER payment {whoSend} -> {amount}")
        querys = f"""    
        INSERT fraud_detector(
             DATE, TIME, PAYMENTTYPE, AMOUNT, WHO
        ) VALUES ("{date}", "{time}", "{paymentType}", {amount}, "{whoSend}"
            )        
        """
        push_sql(querys)