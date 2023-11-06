from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 5),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pension_sales_dashboard',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

# Task 정의
def generate_virtual_payment_data():
    # 가상 결제 데이터 생성 로직
    pass

def send_to_kafka():
    # Kafka로 데이터 보내는 로직
    pass

def aggregate_with_spark():
    # Spark를 이용해 데이터 집계하는 로직
    pass

# PythonOperator를 사용하여 Task를 DAG에 할당
t1 = PythonOperator(
    task_id='generate_virtual_payment_data',
    python_callable=generate_virtual_payment_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='send_to_kafka',
    python_callable=send_to_kafka,
    dag=dag,
)

t3 = PythonOperator(
    task_id='aggregate_with_spark',
    python_callable=aggregate_with_spark,
    dag=dag,
)

# Task 종속성 설정
t1 >> t2 >> t3
