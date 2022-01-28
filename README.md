# kafka_streaming_processing_project

# 프로젝트 전체 process
![설명1-1](https://user-images.githubusercontent.com/60678531/151479212-602ebafa-5f7f-49dd-b665-186a04f1cbb2.png)
--------
![ezgif com-gif-maker](https://user-images.githubusercontent.com/60678531/151478543-1055108b-5c43-4f2e-86bd-4490be3d8263.gif)


## 각 Plane 설명

### 1.payment_producer.py 
```
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

```
1. docker-compose.yml에서 정의한 brokers을 producer에 연결한다. 
2. 시계열 데이터 생성부분 서울시간 기준으로 데이터를 생성한다.
3. 가상 거래 데이터 생성부분은 visa, mastercard, bitcoin 3가지로 정해서 랜덤하게 뽑는다.
4. 가상 데이터를 Stream data로 보내진다고 가정하고 producer로 보낸다.




### 2. fraud_detector.py 

```
from ensurepip import bootstrap
from kafka import KafkaConsumer, KafkaProducer 
import json 

brokers =["localhost:9091", "localhost:9092", "localhost:9093"]
paymentTopicName = "payments"

consumer = KafkaConsumer(paymentTopicName, bootstrap_servers = brokers)
# 정상 비정상 체크 후 하부 서비스로 메세지를 릴레이 해주기 
# producer를 만들어주고 두개 정상 비정상 topic를 만들어주자. 
producer =KafkaProducer(bootstrap_servers = brokers)
topicList = ["fraud_payments", "legit_topic"]

## 비정상 데이터 처리 
## 가상 시나리오 비트코인으로 거래하고 stranger로 보내진 데이터가 비정상 데이터라고 판단

def is_suspicious(transactions):
    if transactions["PAYMENTTYPE"] == "BITCOIN":
        return True
    else:
        return False

for messages in consumer:
    msg = json.loads(messages.value.decode())
    print(msg)
    paymentType = msg["PAYMENTTYPE"]
    payAmount = msg["AMOUNT"]
    isPosible = is_suspicious(msg)
    #  비정상인경우 fraud_payments topic으로 정상인경우 legit_topic으로 전송
    topic = topicList[0] if isPosible else topicList[1]
    producer.send(topic, json.dumps(msg).encode("utf-8"))
    print(f"결제 수단 {paymentType}, 결제 금액: {payAmount}, 비정상 여부: {isPosible}")

```
1. payment_producer.py에서 데이터를 보냈으니 consumer로 받아오는 부분이다.
2. topicList는 비정상 데이터 탐지 topic과 정상 데이터 탐지 topic를 가지고 있다.
3. is_suspicious 함수는 BITCOIN으로 거래된 데이터를 탐지한다.
4. cosumer를 통해서 데이터를 받아오는 부분 
5. topic를 통해 비정상 데이터일 경우 fraud_payment 토픽으로 정상 데이터일 경우 legit_topic 토픽으로 데이터를 다시 producer로 보낸다. 


### 3. fraud_processor.py 

```
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
```
1. fraud_detector.py에서 비정상으로 보낸 topic을 consumer로 받아오는 부분이다.
2. 거래가 비트코인으로 되어있고 상대방이 stranger이면 비정상인 데이터 탐지를 하는 부분이다. 


### 4. legit_processor.py

```
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
```
1. fraud_detector.py에서 정상인 데이터를 producer로 보낸 데이터를 consumer로 받아오는 부분이다. 



## 추가계획
- 정상데이터와 비정상데이터를 mysql에 테이블에 저장하는것을 구현해보자.


