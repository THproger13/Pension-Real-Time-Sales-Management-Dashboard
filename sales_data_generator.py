from datetime import datetime, timedelta
import random
import json

# 상이한 이메일 주소 목록을 생성 한다.
member_emails = [f"user{i}@example.com" for i in range(1, 1000001)]

# 방 종류와 가격을 정의 한다.
room_types = {
    "standard": 100_000,
    "deluxe": 150_000,
    "suite": 200_000,
    "presidential": 300_000
}

# 숙박 인원수 옵션을 정의 한다.
guest_numbers = [2, 3, 4, 5]


# 요일과 시간에 따라 num_transactions 수를 조절 한다.
def modify_num_transactions_as_time_and_weekday():
    current_hour = datetime.now().hour
    current_weekday = datetime.now().weekday()

    # 기본값으로 일단 100을 설정.
    num_transactions = 100

    # 평일 오전 9시부터 오후 6시 사이, 나머지 시간에 대해 결제 데이터 생성량을 다르게 한다.
    if 0 <= current_weekday <= 4:
        if 9 <= current_hour <= 18:
            num_transactions = random.choice(range(100, 300))
        else:
            num_transactions = random.choice(range(200, 600))

    # 주말
    else:
        if 9 <= current_hour <= 12:
            num_transactions = random.choice(range(10, 200))
        elif 12 < current_hour <= 20:
            num_transactions = random.choice(range(150, 700))
        else:
            num_transactions = random.choice(range(0, 100))

    # num_transactions 반환
    return num_transactions


def generate_transactions(num_transactions, member_emails, room_types, guest_numbers):
    transactions = []
    for _ in range(num_transactions):
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
num_transactions = modify_num_transactions_as_time_and_weekday()
example_transactions = generate_transactions(num_transactions, member_emails, room_types, guest_numbers)
for transaction in example_transactions:
    print(transaction)
