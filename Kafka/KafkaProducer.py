from fastapi import FastAPI
from kafka import KafkaProducer
import json

app = FastAPI()
producer = KafkaProducer(bootstrap_servers='kafka-server:9092')

@app.post("/generate-transactions/")
async def generate_transactions():
    transactions = []  # 여기서 실제 데이터를 생성하는 로직을 구현해야 합니다.
    for transaction in transactions:
        # Kafka 토픽으로 데이터를 JSON 형식으로 전송합니다.
        producer.send('pension-sales', json.dumps(transaction).encode('utf-8'))
    return {"status": "Transactions sent to Kafka"}
