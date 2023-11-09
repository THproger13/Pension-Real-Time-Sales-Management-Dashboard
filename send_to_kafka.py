# from fastapi import FastAPI
from kafka import KafkaProducer
import json
from sales_data_generator import modify_num_transactions_as_time_and_weekday, generate_transactions
from sales_data_generator import member_emails, room_types, guest_numbers


# app = FastAPI()
producer = KafkaProducer(bootstrap_servers='kafka-server:9092')

def send_to_kafka():
    # generate_transactions를 호출하여 트랜잭션 데이터 생성
    num_transactions = modify_num_transactions_as_time_and_weekday()
    transactions = generate_transactions(num_transactions, member_emails, room_types, guest_numbers)

    # 생성된 각 트랜잭션을 Kafka로 전송
    for transaction in transactions:
        producer.send('pension-sales', json.dumps(transaction).encode('utf-8'))
