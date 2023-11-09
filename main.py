from fastapi import FastAPI
import json
from kafka import KafkaProducer

app = FastAPI()

# 기존 엔드포인트
@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}

@app.post("/generate-transactions/")
async def generate_transactions():
    # Kafka 토픽 이름을 지정합니다.
    topic_name = 'pension-sales'
    # send_to_kafka(topic_name)
    return {"status": "Transactions sent to Kafka"}
