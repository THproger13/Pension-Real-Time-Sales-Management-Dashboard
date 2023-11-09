from fastapi import FastAPI
import json

app = FastAPI()

# 기존 엔드포인트
@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}

# 새로운 엔드포인트: transactions.json 파일로부터 데이터를 읽어서 반환
# @app.get("/transactions")
# def read_transactions():
#     with open('data/transactions.json', 'r') as f:  # 파일 경로 확인 필요
#         transactions = json.load(f)
#     return transactions

@app.post("/generate-transactions/")
async def generate_transactions():
    # Kafka 토픽 이름을 지정합니다.
    topic_name = 'pension-sales'
    send_transactions_to_kafka(topic_name)
    return {"status": "Transactions sent to Kafka"}