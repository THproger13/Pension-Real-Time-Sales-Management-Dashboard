from fastapi import FastAPI, HTTPException
from kafka.producer import KafkaProducer
import json
from sales_data_generator import generate_transactions, member_emails, room_types, guest_numbers, \
    modify_num_transactions_as_time_and_weekday
from aggregate_with_spark import aggregate_with_spark
from send_to_kafka import send_to_kafka

app = FastAPI()


# 기존 엔드포인트
@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


# @app.post("/test/generate_transactions")
# def test_generate_transactions(num_transactions: int):
#     try:
#         transactions = generate_transactions(num_transactions, member_emails, room_types, guest_numbers)
#         return {"transactions": transactions}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

@app.post("/test/generate_transactions")
def test_generate_transactions():
    # def test_generate_transactions():
    try:
        # modify_num_transactions_as_time_and_weekday 함수를 호출하여 num_transactions 값을 조정합니다.
        adjusted_num_transactions = modify_num_transactions_as_time_and_weekday()
        transactions = generate_transactions(adjusted_num_transactions, member_emails, room_types, guest_numbers)
        return {"transactions": transactions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# @app.post("/test/send_to_kafka")
# def test_send_to_kafka():
#     try:
#         adjusted_num_transactions = modify_num_transactions_as_time_and_weekday()
#         transactions = generate_transactions(adjusted_num_transactions, member_emails, room_types, guest_numbers)
#         kafkaresult = send_to_kafka(transactions)
#         if kafkaresult:
#             return {"Sent transactions to Kafka": transactions}
#         else:
#             return {"kafka error": "뿌엥 ㅜㅜㅜ"}
#         # return send_to_kafka(transactions)
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

@app.post("/test/send_to_kafka")
def test_send_to_kafka():
    try:
        adjusted_num_transactions = modify_num_transactions_as_time_and_weekday()
        transactions = generate_transactions(adjusted_num_transactions, member_emails, room_types, guest_numbers)
        kafka_result = send_to_kafka(transactions)

        if kafka_result["failure"] == 0:
            return {"message": "All transactions sent to Kafka successfully", "details": kafka_result}
        else:
            return {"message": "Some transactions failed to send", "details": kafka_result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/test/aggregate_with_spark")
def test_aggregate_with_spark():
    try:
        adjusted_num_transactions = modify_num_transactions_as_time_and_weekday()
        transactions = generate_transactions(adjusted_num_transactions, member_emails, room_types, guest_numbers)
        send_to_kafka(transactions)
        result = aggregate_with_spark()
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
