import logging
import time
from datetime import datetime, timedelta
import random
from send_to_kafka import divide_transactions, run_producer_threads
from aggregate_with_spark import aggregate_with_spark
# import json

# 상이한 이메일 주소 목록을 생성 한다.
member_emails = [f"user{i}@example.com" for i in range(1, 100000001)]

# 방 종류와 가격을 정의 한다.
room_types = {
    "standard": 100_000,
    "deluxe": 150_000,
    "suite": 200_000,
    "presidential": 300_000
}

# 숙박 인원수 옵션을 정의 한다.
guest_numbers = [2, 3, 4, 5]

def is_discount_promotion():
    current_date = datetime.now()
    # 할인 프로모션 날짜 범위 설정
    promo_start = datetime(2023, 11, 15)
    promo_end = datetime(2023, 11, 30)
    return promo_start <= current_date <= promo_end


# 요일과 시간에 따라 num_transactions 수를 조절 한다.
def modify_num_transactions_as_time_and_weekday():
    current_hour = datetime.now().hour
    current_weekday = datetime.now().weekday()
    current_month = datetime.now().month

    # 기본값으로 일단 100을 설정.
    num_transactions = 100

    # 평일 오전 9시부터 오후 6시 사이, 나머지 시간에 대해 결제 데이터 생성량을 다르게 한다.
    if 0 <= current_weekday <= 4:
        if 9 <= current_hour <= 18:
            num_transactions = random.choice(range(1000, 3000))
        else:
            num_transactions = random.choice(range(2000, 6000))

    # 주말
    else:
        if 9 <= current_hour <= 12:
            num_transactions = random.choice(range(1000, 2000))
        elif 12 < current_hour <= 20:
            num_transactions = random.choice(range(1500, 7000))
        else:
            num_transactions = random.choice(range(0, 1000))

    # 성수기 조건
    if current_month in [6, 7, 8]:
        num_transactions *= 1.5  # 성수기에는 50% 증가

    # 비성수기 조건
    elif current_month in [1, 2, 3, 4, 5, 9, 10, 11, 12]:
        num_transactions *= 0.7  # 비성수기에는 30% 감소

    # 할인 프로모션 조건
    if is_discount_promotion():
        num_transactions *= 1.2  # 할인 프로모션에는 20% 증가

    # num_transactions 반환
    return num_transactions


def generate_transactions(num_transactions, member_emails, room_types, guest_numbers):
    rounded_num_transaction = round(num_transactions)
    transactions = []
    for _ in range(rounded_num_transaction):
        # 임의의 멤버 이메일을 선택.
        member_email = random.choice(member_emails)
        # 임의의 방 종류를 선택하고, 해당 가격을 가져옴.
        room_type, room_price = random.choice(list(room_types.items()))
        # 임의의 인원수를 선택.
        guests = random.choice(guest_numbers)

        # 결제 데이터를 생성.
        transaction = {
            "memberEmail": member_email,
            "roomType": room_type,
            "roomPrice": room_price,
            "guestNumber": guests,
            "timestamp": datetime.now().isoformat()  # 현재 시각을 ISO 형식으로 추가.
        }
        transactions.append(transaction)

    return transactions


# generate_transactions를 호출하기 전에 num_transactions 값을 설정.
while True:
    num_transactions = modify_num_transactions_as_time_and_weekday()
    print("num_transactions : ", num_transactions)
    total_transactions = generate_transactions(num_transactions, member_emails, room_types, guest_numbers)
    num_threads = 10 # 스레드 수
    transactions_per_thread = divide_transactions(total_transactions, num_threads)  # 각 스레드에 할당될 트랜잭션 목록
    start_time = time.time()  # 루프 시작 시간 기록
    run_producer_threads(num_threads, transactions_per_thread)

    # 메시지 전송에 걸린 시간을 계산합니다.
    elapsed_time = time.time() - start_time
    print(f"Elapsed time for sending messages: {elapsed_time:.2f} seconds")

    # 메시지 전송 시간이 1초보다 적게 걸렸다면, 나머지 시간만큼 대기합니다.
    if elapsed_time < 1.0:
        time.sleep(1 - elapsed_time)

    # for transaction in example_transactions:
            # print(transaction)

