# from fastapi import FastAPI
import logging

from kafka.producer import KafkaProducer
import json

# from sales_data_generator import modify_num_transactions_as_time_and_weekday, generate_transactions
# from sales_data_generator import member_emails, room_types, guest_numbers

# app = FastAPI()
logging.basicConfig(level=logging.INFO)


# def send_to_kafka(transactions):
#     # KafkaProducer 인스턴스 생성
#     producer = KafkaProducer(bootstrap_servers='172.28.31.155:9092')
#     # producer = KafkaProducer(bootstrap_servers='kafka:9092')
#
#     # 생성된 각 트랜잭션을 Kafka로 전송
#     for transaction in transactions:
#         producer.send('pension-sales', json.dumps(transaction).encode('utf-8'))
#         # result = future.get(timeout=3)
#
#         # 결과를 로깅
#         # if result:
#         #     logging.info(f"Transaction sent to Kafka: {transaction}")
#         # else:
#         #     logging.error("Failed to send transaction to Kafka")
#
#     # 모든 메시지가 전송될 때까지 기다림
#     producer.flush()
#     # print(f"Sent transactions to Kafka: {transactions}")



# def send_to_kafka(transactions):
#     producer = KafkaProducer(bootstrap_servers='172.28.31.155:9092')
#     success_count = 0
#     failure_count = 0
#
#     for transaction in transactions:
#         try:
#             # Kafka로 데이터 전송 및 결과 확인
#             future = producer.send('pension-sales', json.dumps(transaction).encode('utf-8'))
#             result = future.get(timeout=3)  # 타임아웃 설정
#             if result:
#                 success_count += 1
#             else:
#                 failure_count += 1
#         except Exception as e:
#             failure_count += 1
#             logging.error(f"Failed to send transaction to Kafka: {e}")
#
#     producer.flush()
#
#     return {"success": success_count, "failure": failure_count}


# def on_send_success(record_metadata):
#     logging.info(
#         f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
#
#
# def on_send_error(excp):
#     logging.error('I am an errback', exc_info=excp)
#
#
# def send_to_kafka(transactions):
#     producer = KafkaProducer(bootstrap_servers='172.28.31.155:9092',
#                              value_serializer=lambda x: json.dumps(x).encode('utf-8'))
#
#     for transaction in transactions:
#         producer.send('pension-sales', transaction).add_callback(on_send_success).add_errback(on_send_error)
#
#     # 모든 메시지가 전송될 때까지 기다림
#     producer.flush()


class KafkaResult:
    success_count = 0
    failure_count = 0

def on_send_success(record_metadata):
    KafkaResult.success_count += 1
    logging.info(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    KafkaResult.failure_count += 1
    logging.error('Message send failed', exc_info=excp)

def send_to_kafka(transactions):
    producer = KafkaProducer(bootstrap_servers='172.28.31.155:9092',
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                             api_version=(0,11,5)
    )

    for transaction in transactions:
        producer.send('pension-sales', transaction).add_callback(on_send_success).add_errback(on_send_error)

    producer.flush()

    print("total:", len(transactions), "success:", KafkaResult.success_count, "failure:", KafkaResult.failure_count)

