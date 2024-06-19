from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os

# Função que será executada pela DAG
def run_import_csv_to_new_db():
    import os
    import pandas as pd
    import psycopg2
    from datetime import datetime
    from psycopg2 import sql

    db_config = {'host': 'localhost', 'database': 'northwind_datamart', 'user': 'airflow'}
    os.environ['PGPASSWORD'] = 'passairflow'

    def connect_db():
        conn = psycopg2.connect(host=db_config['host'], dbname=db_config['database'], user=db_config['user'])
        return conn

    tables = ['categories', 'products', 'suppliers', 'employees',
              'employee_territories', 'order_details', 'orders',
              'customers', 'customer_customer_demo', 'customer_demographics',
              'shippers', 'us_states']

    def import_csv_to_table(csv_file, table_name, conn):
        df = pd.read_csv(csv_file)
        cursor = conn.cursor()

        columns = df.columns
        query = sql.SQL("COPY {table} ({fields}) FROM STDIN WITH CSV HEADER").format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns))
        )

        with open(csv_file, 'r') as f:
            cursor.copy_expert(query, f)

        conn.commit()
        cursor.close()

    # Usando a variável IMPORT_FROM_DATE do Airflow
    folder_date = Variable.get("IMPORT_FROM_DATE", default_var=datetime.now().strftime('%Y-%m-%d'))

    folder_path = f'/tmp/csv/{folder_date}/'
    if not os.path.exists(folder_path):
        print(f"A pasta {folder_path} não existe.")
        return

    conn = connect_db()

    for table in tables:
        csv_file_path = os.path.join(folder_path, f'{table}.csv')
        if os.path.exists(csv_file_path):
            print(f"Importando {csv_file_path} para a tabela {table}...")
            import_csv_to_table(csv_file_path, table, conn)
        else:
            print(f"Arquivo {csv_file_path} não encontrado.")

    conn.close()
    print("Importação concluída.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 17),
    'retries': 1,
}

dag = DAG(
    'dag_import_csv_to_new_db',
    default_args=default_args,
    description='Importa dados de CSV para novo banco de dados',
    schedule_interval=timedelta(days=1),  # Diário
    catchup=False,
)

# Tarefa que executa a função definida
import_csv_task = PythonOperator(
    task_id='import_csv_to_new_db',
    python_callable=run_import_csv_to_new_db,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()