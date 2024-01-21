# from fastapi import FastAPI
import logging
import threading
import time

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


producer = KafkaProducer(bootstrap_servers='172.28.31.155:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                         api_version=(0, 11, 5),

                         # 성능 향상을 위한 설정
                         batch_size=128 * 1024,  # 32KB; 배치 크기 조절
                         linger_ms=5,  # 10ms; 린지 모드
                         buffer_memory=128 * 1024 * 1024,  # 64MB; 버퍼 메모리 조정
                         compression_type='gzip',  # 메시지 압축 사용

                         # reconnect_backoff_ms=500,  # 재연결 시도 간의 대기 시간 (밀리초)
                         # reconnect_backoff_max_ms=1000,  # 최대 재연결 대기 시간 (밀리초)
                         # connections_max_idle_ms=10 * 60 * 1000  # 연결 유휴 시간 (10분으로 설정)
                         # acks='1'
                         )
def send_to_kafka(transactions):
    global producer
    try:
        for transaction in transactions:
            producer.send('pension-sales', transaction).add_callback(on_send_success).add_errback(on_send_error)
        producer.flush()
    except Exception as e:
        logging.error('Exception in send_to_kafka', exc_info=e)

def run_producer_threads(thread_count, transactions_per_thread):
    start_time = time.time()

    threads = []
    for i in range(thread_count):
        # thread_transactions = transactions_per_thread
        thread = threading.Thread(target=send_to_kafka, args=(transactions_per_thread[i], ))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # producer.close()  # 모든 스레드가 종료된 후에 프로듀서 인스턴스를 닫습니다.

    end_time = time.time()  # 메시지 전송 완료 시간 기록
    total_time = end_time - start_time  # 전체 소요 시간 계산
    print(f"Total time to send messages: {total_time} seconds")


def divide_transactions(total_transactions, num_threads):
    """전체 트랜잭션을 스레드 수에 따라 분할합니다."""
    # 각 스레드에 할당될 트랜잭션 수를 계산합니다.
    per_thread = len(total_transactions) // num_threads
    return [total_transactions[i * per_thread:(i + 1) * per_thread] for i in range(num_threads)]


#     per_thread = len(total_transactions) // num_threads
#     remainder = len(total_transactions) % num_threads
#     transactions_per_thread = [total_transactions[i * per_thread:(i + 1) * per_thread] for i in range(num_threads)]
#
#     # 나머지 트랜잭션들을 마지막 스레드에 추가
#     if remainder:
#         transactions_per_thread[-1].extend(total_transactions[-remainder:])
# def divide_transactions(total_transactions, num_threads):


#
#     return transactions_per_thread

# 전체 트랜잭션 데이터를 생성합니다.
# total_transactions

# 예제 데이터 및 실행 코드


# print("total:", len(total_transactions), "success:", KafkaResult.success_count, "failure:", KafkaResult.failure_count)
