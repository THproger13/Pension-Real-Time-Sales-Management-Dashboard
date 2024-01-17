# from fastapi import FastAPI
import logging
import threading

from kafka.producer import KafkaProducer
import json

# from sales_data_generator import modify_num_transactions_as_time_and_weekday, generate_transactions
# from sales_data_generator import member_emails, room_types, guest_numbers

# app = FastAPI()


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

logging.basicConfig(level=logging.INFO)


class KafkaResult:
    success_count = 0
    failure_count = 0


def on_send_success(record_metadata):
    KafkaResult.success_count += 1
    logging.info(
        f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")


def on_send_error(excp):
    KafkaResult.failure_count += 1
    logging.error('Message send failed', exc_info=excp)


def send_to_kafka(transactions):
    producer = KafkaProducer(bootstrap_servers='172.28.31.155:9092',
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                             api_version=(0, 11, 5),

                             # 성능 향상을 위한 설정
                             batch_size=32 * 1024,  # 32KB; 배치 크기 조절
                             linger_ms=10,  # 10ms; 린지 모드
                             buffer_memory=64 * 1024 * 1024,  # 64MB; 버퍼 메모리 조정
                             compression_type='gzip',  # 메시지 압축 사용
                             # acks='1'
                             )

    for transaction in transactions:
        producer.send('pension-sales', transaction).add_callback(on_send_success).add_errback(on_send_error)

    producer.flush


def run_producer_threads(thread_count, transactions_per_thread):
    threads = []
    for _ in range(thread_count):
        thread_transactions = transactions_per_thread.copy()
        thread = threading.Thread(target=send_to_kafka, args=(thread_transactions, ))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def divide_transactions(total_transactions, num_threads):
    """전체 트랜잭션을 스레드 수에 따라 분할합니다."""
    # 각 스레드에 할당될 트랜잭션 수를 계산합니다.
    per_thread = len(total_transactions) // num_threads
    return [total_transactions[i * per_thread:(i + 1) * per_thread] for i in range(num_threads)]

# def divide_transactions(total_transactions, num_threads):
#     per_thread = len(total_transactions) // num_threads
#     remainder = len(total_transactions) % num_threads
#     transactions_per_thread = [total_transactions[i * per_thread:(i + 1) * per_thread] for i in range(num_threads)]
#
#     # 나머지 트랜잭션들을 마지막 스레드에 추가
#     if remainder:
#         transactions_per_thread[-1].extend(total_transactions[-remainder:])
#
#     return transactions_per_thread

# 전체 트랜잭션 데이터를 생성합니다.
# total_transactions

# 예제 데이터 및 실행 코드


# print("total:", len(total_transactions), "success:", KafkaResult.success_count, "failure:", KafkaResult.failure_count)
