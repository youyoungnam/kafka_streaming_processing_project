import datetime
from ensurepip import bootstrap
import pytz 
import time 
import random 
import json
from kafka import KafkaProducer

# broker들의 ports Lists
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
producers = KafkaProducer(bootstrap_servers=brokers)
topicName = "payments"


# 시계열 데이터 생성 
def get_time_date():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    kst_now = utc_now.astimezone(pytz.timezone("Asia/Seoul"))
    days = kst_now.strftime("%m/%d/%Y")
    times = kst_now.strftime("%H:%M:%S")
    return days, times 

# 가상 거래 데이터 생성
def generate_payment_data():
    paymentType = random.choice(["VISA", "MASTERCARD", "BITCOIN"])
    amount = random.randint(0, 100)
    to = random.choice(["ME", "MOM", "DAD", "FRIEND", "STRANGER"])
    return paymentType, amount, to 


# 스트림으로 데이터가 보내진다고 가정 

while True:
    days, times = get_time_date()
    paymentType, amount, to = generate_payment_data()
    
    virtual_data = {
        "DATE": days,
        "TIME": times,
        "PAYMENTTYPE":paymentType,
        "AMOUNT": amount,
        "WHO": to 
        }
    time.sleep(1.5)    
    print(f"가상데이터 스트림 데이터전송: {virtual_data}")
    # 파이썬 객체로 브로커 내부 토픽에 보내면안됨 
    producers.send(topicName, json.dumps(virtual_data).encode("utf-8"))
