from datetime import datetime, timedelta
import random

# 상이한 이메일 주소 목록을 생성합니다.
# 실제 환경에서는 이 부분을 데이터베이스나 파일에서 불러올 것입니다.
member_emails = [f"user{i}@example.com" for i in range(1, 1000001)]

# 방 종류와 가격을 정의합니다.
room_types = {
    "standard": 100.000,
    "deluxe": 150.000,
    "suite": 200.000,
    "presidential": 300.000
}

# 숙박 인원수 옵션을 정의합니다.
guest_numbers = [2, 3, 4, 5]

def generate_transactions(num_transactions, member_emails, room_types, guest_numbers):
    transactions = []
    for _ in range(num_transactions):
        # 임의의 멤버 이메일을 선택합니다.
        member_email = random.choice(member_emails)
        # 임의의 방 종류를 선택하고, 해당 가격을 가져옵니다.
        room_type, room_price = random.choice(list(room_types.items()))
        # 임의의 인원수를 선택합니다.
        guests = random.choice(guest_numbers)

        # 결제 데이터를 생성합니다.
        transaction = {
            "memberEmail": member_email,
            "roomType": room_type,
            "roomPrice": room_price,
            "guestNumber": guests,
            "timestamp": datetime.now().isoformat()  # 현재 시각을 ISO 형식으로 추가합니다.
        }
        transactions.append(transaction)

    return transactions

# 예시로 10개의 결제 데이터를 생성합니다.
# 실제로는 각 고객이 3~4번 결제한다고 하니, 이를 반영하여 적당한 수를 곱해줍니다.
example_transactions = generate_transactions(10 * 3, member_emails, room_types, guest_numbers)
for transaction in example_transactions:
    print(transaction)
