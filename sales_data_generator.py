import json
import random
from datetime import datetime, timedelta

# 결제 데이터 예제 구조
def generate_transaction(user_id, product_id):
    timestamp = datetime.now() - timedelta(days=random.randint(0, 365))
    return {
        "user_id": user_id,
        "product_id": product_id,
        "amount": round(random.uniform(10.0, 500.0), 2),
        "timestamp": timestamp.isoformat()
    }

# 가상 결제 데이터 생성
def generate_data(num_transactions):
    transactions = []
    for _ in range(num_transactions):
        user_id = random.randint(1, 1000)
        product_id = random.randint(1, 100)
        transactions.append(generate_transaction(user_id, product_id))
    return transactions

if __name__ == "__main__":
    num_transactions = 1000  # 생성할 가상 결제 데이터의 수
    transactions = generate_data(num_transactions)
    with open('app/data/transactions.json', 'w') as f:
        json.dump(transactions, f, indent=4)
