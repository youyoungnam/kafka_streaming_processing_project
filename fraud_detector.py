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
