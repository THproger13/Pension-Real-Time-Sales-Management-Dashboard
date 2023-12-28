from fastapi import FastAPI, HTTPException
from kafka.producer import KafkaProducer
import json
from sales_data_generator import generate_transactions, member_emails, room_types, guest_numbers
from aggregate_with_spark import aggregate_with_spark

app = FastAPI()

# 기존 엔드포인트
@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}

# @app.post("/generate-transactions/")
# async def generate_transactions():
#     # Kafka 토픽 이름을 지정합니다.
#     topic_name = 'pension-sales'
#     # send_to_kafka(topic_name)
#     return {"status": "Transactions sent to Kafka"}

@app.post("/test/generate_transactions")
def test_generate_transactions(num_transactions: int):
    try:
        transactions = generate_transactions(num_transactions, member_emails, room_types, guest_numbers)
        return {"transactions": transactions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test/send_to_kafka")
def test_send_to_kafka(transactions: list):
    try:
        producer = KafkaProducer(bootstrap_servers='172.28.31.155:9092')
        for transaction in transactions:
            producer.send('pension-sales', json.dumps(transaction).encode('utf-8'))
        producer.flush()
        return {"status": "Sent to Kafka"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test/aggregate_with_spark")
def test_aggregate_with_spark():
    try:
        result = aggregate_with_spark()
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))