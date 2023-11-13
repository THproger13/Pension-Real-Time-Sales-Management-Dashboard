import sys

sys.path.insert(0, '/mnt/c/data1/(Python)Pension-Real-Time-Sales-Management-Dashboard/')

from datetime import datetime, timedelta
from airflow import DAG
# Airflow 2.0 이상에서 PythonOperator 임포트 방법
from airflow.operators.python import PythonOperator

# DummyOperator는 빨간줄이 떠서 일단 사용하지 않기로 결정함.
# 추후 task 그룹화가 필수적이 되면 그때 해결하자.

# from airflow.operators.dummy import DummyOperator

from sales_data_generator import modify_num_transactions_as_time_and_weekday, generate_transactions
from sales_data_generator import member_emails, room_types, guest_numbers

from send_to_kafka import send_to_kafka
from aggregate_with_spark import aggregate_with_spark

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 12),
    'email': ['thphysics@naver.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=1.5),
}

dag = DAG(
    'kafka_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline that sends data to Kafka and aggregates with Spark',
    schedule=timedelta(days=1),  # 일단위 스케줄
)


# 할인 프로모션 여부를 결정하는 함수(boolean type을 리턴한다.)
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
    elif today.month in [1, 2, 3, 4, 5, 9, 10, 11, 12]:
        num_transactions *= 0.7  # 비성수기에는 30% 감소

    # 할인 프로모션 조건
    if is_discount_promotion(today):
        num_transactions *= 1.2  # 할인 프로모션에는 20% 증가

    kwargs['ti'].xcom_push('num_transactions', num_transactions)


# 성수기/비성수기 가상 결제 데이터 생성 Task 정의
def create_data(**kwargs):
    num_transactions = kwargs['ti'].xcom_pull(task_ids='adjust_transactions_for_event') or 100
    transactions = generate_transactions(num_transactions, member_emails, room_types, guest_numbers)
    # 여기에서 Kafka로 보낼 데이터를 추가적으로 처리할 수 있다.
    send_to_kafka(transactions)


# num_transactions 조절 Task
adjust_transactions_for_event_task = PythonOperator(
    task_id='adjust_transactions_for_event',
    python_callable=adjust_transactions_for_event,
    # provide_context=True,
    dag=dag,
)

# 데이터 생성 Task
generate_virtual_payment_data_task = PythonOperator(
    task_id='generate_virtual_payment_data',
    python_callable=create_data,
    provide_context=True,
    dag=dag,
)


# Kafka로 데이터 보내기 Task 정의
# def send_to_kafka_wrapper(**kwargs):
#     transactions = kwargs['ti'].xcom_pull(task_ids='generate_virtual_payment_data')
#     if transactions is None:
#         raise ValueError("No transactions data found.")
#     send_to_kafka(transactions)


# 데이터를 Kafka로 보내는 Task (여기서는 send_to_kafka 함수를 정의해야 합니다)
# send_to_kafka_task = PythonOperator(
#     task_id='send_to_kafka',
#     python_callable=send_to_kafka_wrapper,  # 이 함수는 정의 되어 있어야 합니다.
#     provide_context=True,
#     dag=dag,
# )

# Spark를 이용해 데이터 집계하는 Task (여기서는 aggregate_with_spark 함수를 정의해야 합니다)
aggregate_with_spark_task = PythonOperator(
    task_id='aggregate_with_spark',
    python_callable=aggregate_with_spark,  # 이 함수는 정의되어 있어야 합니다.
    # provide_context=True,
    dag=dag,
)

# 종료 Task 정의
# end_operator = DummyOperator(task_id='end_execution', dag=dag)

# Task 종속성 설정
# adjust_transactions_for_event_task >> generate_virtual_payment_data_task
# generate_virtual_payment_data_task >> send_to_kafka_task
# send_to_kafka_task >> aggregate_with_spark_task
# aggregate_with_spark >> end_operator

adjust_transactions_for_event_task >> generate_virtual_payment_data_task
generate_virtual_payment_data_task >> aggregate_with_spark_task
