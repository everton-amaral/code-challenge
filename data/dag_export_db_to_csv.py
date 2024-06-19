from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2

db_config = {'host': 'localhost','database': 'northwind','user': 'airflow'}
os.environ['PGPASSWORD'] = 'passairflow'

#Tabelas que serão exportadas para CSV
tables = [
    'categories', 'products', 'suppliers', 'employees',
    'employee_territories', 'order_details', 'orders',
    'customers', 'customer_customer_demo', 'customer_demographics',
    'shippers', 'us_states'
]

# Função para exportar tabela para CSV
def export_to_csv(table_name, file_path):
    conn = psycopg2.connect(**db_config)
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, conn)
    df.to_csv(file_path, index=False)
    conn.close()

#Função principal que será executada pela DAG
def export_tables_to_csv():
    today_date = datetime.now().strftime('%Y-%m-%d')
    csv_folder = f'/tmp/csv/{today_date}/'  #TMP na minha máquina local, só para não armazenar atoa

    # Criando a pasta para o dia atual
    os.makedirs(csv_folder, exist_ok=True)
    
    #Exportando cada tabela para CSV
    for table in tables:
        csv_file = f'{csv_folder}/{table}.csv'
        export_to_csv(table, csv_file)
        print(f'Tabela {table} exportada para {csv_file}')

# Definindo os argumentos da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_export_db_to_csv',
    default_args=default_args,
    description='Exporta tabelas do banco de dados para CSV diariamente',
    schedule_interval=timedelta(days=1),  #Diário
    catchup=False,
)

#Tarefa na DAG para executar a função principal
export_to_csv_task = PythonOperator(
    task_id='export_to_csv_task',
    python_callable=export_tables_to_csv,
    dag=dag,
)