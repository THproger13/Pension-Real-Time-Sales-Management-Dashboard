import json
import sys

# 트랜잭션 데이터 예시
transaction = {
    "memberEmail": "user123451247@example.com",
    "roomType": "standard",
    "roomPrice": 100000,
    "guestNumber": 3,
    "timestamp": "2024-01-01T12:00:00 3424634"
}

# JSON 문자열로 변환
json_data = json.dumps(transaction)

# 바이트 크기 계산
byte_size = sys.getsizeof(json_data)

print(f"JSON 데이터 크기: {byte_size} 바이트")
