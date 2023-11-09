from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# generate_data.py 모듈에서 함수들을 임포트합니다.
from sales_data_generator import modify_num_transactions_as_time_and_weekday, generate_transactions

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 8),
    'email': ['thphysics@naver.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'pension_sales_dashboard',
    default_args=default_args,
    description='A DAG for scheduling virtual payment data generation based on seasonality and promotions',
    schedule_interval=timedelta(days=1),  # 매일 실행을 위한 설정
)

# 할인 프로모션 여부를 결정하는 함수
def is_discount_promotion(current_date):
    # 할인 프로모션 날짜 범위 설정
    promo_start = datetime(2023, 11, 15)
    promo_end = datetime(2023, 11, 30)
    return promo_start <= current_date <= promo_end

# 이벤트 기반으로 num_transactions 조절하는 함수
def adjust_transactions_for_event(**kwargs):
    num_transactions = modify_num_transactions_as_time_and_weekday()  # 기본값 설정
    today = datetime.now()

    # 성수기 조건
    if today.month in [6, 7, 8]:
        num_transactions *= 1.5  # 성수기에는 50% 증가

    # 비성수기 조건
    elif today.month in [4, 5, 9, 10]:
        num_transactions *= 0.7  # 비성수기에는 30% 감소

    # 할인 프로모션 조건
    if is_discount_promotion(today):
        num_transactions *= 1.2  # 할인 프로모션에는 20% 증가

    kwargs['ti'].xcom_push('num_transactions', num_transactions)

# 성수기/비성수기 가상 결제 데이터 생성 Task 정의
def create_data(**kwargs):
    num_transactions = kwargs['ti'].xcom_pull(task_ids='adjust_transactions_for_event')
    transactions = generate_transactions(num_transactions, member_emails, room_types, guest_numbers)
    # 여기에서 Kafka로 보낼 데이터를 추가적으로 처리할 수 있습니다.
    # 예: kafka_producer.send(transactions)

# num_transactions 조절 Task
adjust_transactions_for_event_task = PythonOperator(
    task_id='adjust_transactions_for_event',
    python_callable=adjust_transactions_for_event,
    provide_context=True,
    dag=dag,
)

# 데이터 생성 Task
generate_virtual_payment_data = PythonOperator(
    task_id='generate_virtual_payment_data',
    python_callable=create_data,
    provide_context=True,
    dag=dag,
)

# 데이터를 Kafka로 보내는 Task (여기서는 send_to_kafka 함수를 정의해야 합니다)
send_to_kafka = PythonOperator(
    task_id='send_to_kafka',
    # python_callable=send_to_kafka,  # 이 함수는 정의되어 있어야 합니다.
    provide_context=True,
    dag=dag,
)

# Spark를 이용해 데이터 집계하는 Task (여기서는 aggregate_with_spark 함수를 정의해야 합니다)
aggregate_with_spark = PythonOperator(
    task_id='aggregate_with_spark',
    # python_callable=aggregate_with_spark,  # 이 함수는 정의되어 있어야 합니다.
    provide_context=True,
    dag=dag,
)

# 종료 Task 정의
end_operator = DummyOperator(task_id='end_execution', dag=dag)

# Task 종속성 설정
adjust_transactions_for_event_task >> generate_virtual_payment_data
generate_virtual_payment_data >> send_to_kafka
send_to_kafka >> aggregate_with_spark
aggregate_with_spark >> end_operator

