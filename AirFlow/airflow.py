# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
#
# # DAG 정의
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 11, 5),
#     'email': ['your_email@example.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
#
# dag = DAG(
#     'pension_sales_dashboard',
#     default_args=default_args,
#     description='A simple tutorial DAG',
#     schedule_interval=timedelta(days=1),
# )
#
# # Task 정의
# def generate_virtual_payment_data():
#     # 가상 결제 데이터 생성 로직
#     pass
#
# def send_to_kafka():
#     # Kafka로 데이터 보내는 로직
#     pass
#
# def aggregate_with_spark():
#     # Spark를 이용해 데이터 집계하는 로직
#     pass
#
# # PythonOperator를 사용하여 Task를 DAG에 할당
# t1 = PythonOperator(
#     task_id='generate_virtual_payment_data',
#     python_callable=generate_virtual_payment_data,
#     dag=dag,
# )
#
# t2 = PythonOperator(
#     task_id='send_to_kafka',
#     python_callable=send_to_kafka,
#     dag=dag,
# )
#
# t3 = PythonOperator(
#     task_id='aggregate_with_spark',
#     python_callable=aggregate_with_spark,
#     dag=dag,
# )
#
# # Task 종속성 설정
# t1 >> t2 >> t3


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import random

# 여기에 기존의 코드를 삽입합니다.

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 8),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'pension_sales_dashboard',
    default_args=default_args,
    description='A DAG for scheduling virtual payment data generation based on seasonality and promotions',
    schedule_interval='@daily',  # 매일 실행
)

# 조건부 실행을 위한 함수 정의
def should_run():
    # 현재 날짜나 다른 조건을 기반으로 실행할지 여부를 결정
    # 여기서는 예시로 날짜를 확인하는 로직만 추가
    today = datetime.now()
    if today.month in [6, 7, 8]:  # 여름 성수기 예시
        return 'generate_virtual_payment_data_high_season'
    else:
        return 'generate_virtual_payment_data_low_season'

# 분기 Task 정의
branching = BranchPythonOperator(
    task_id='branching',
    python_callable=should_run,
    dag=dag,
)

# 성수기/비성수기 가상 결제 데이터 생성 Task 정의
generate_data_high_season = PythonOperator(
    task_id='generate_virtual_payment_data_high_season',
    python_callable=generate_transactions,  # 성수기용 데이터 생성 함수
    op_args=[100, member_emails, room_types, guest_numbers],  # 인자 설정
    dag=dag,
)

generate_data_low_season = PythonOperator(
    task_id='generate_virtual_payment_data_low_season',
    python_callable=generate_transactions,  # 비성수기용 데이터 생성 함수
    op_args=[50, member_emails, room_types, guest_numbers],  # 인자 설정
    dag=dag,
)

# Kafka로 데이터 보내는 Task 정의
send_to_kafka = PythonOperator(
    task_id='send_to_kafka',
    python_callable=send_to_kafka,
    dag=dag,
)

# Spark를 이용해 데이터 집계하는 Task 정의
aggregate_with_spark = PythonOperator(
    task_id='aggregate_with_spark',
    python_callable=aggregate_with_spark,
    dag=dag,
)

# 종료 Task 정의
end_operator = DummyOperator(task_id='end_execution', dag=dag)

# Task 종속성 설정
branching >> [generate_data_high_season, generate_data_low_season]
generate_data_high_season >> send_to_kafka
generate_data_low_season >> send_to_kafka
send_to_kafka >> aggregate_with_spark
aggregate_with_spark >> end_operator

