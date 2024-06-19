# iniciando processo de engenharia de dados (ETL)

## Ferramentas utilizadas: Docker, PostgreSQL, python3 c/ pandas e Airflow

Após realizar a instalação do docker, foi criado um banco de dados PostgreSQL na versão 16.

docker-compose.yaml
```version: '3.8'

services:
  db:
    image: postgres:latest
    volumes:
      - /Users/carolrmr/postgresql/data
    environment:
      POSTGRES_DB: northwind
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: passairflow
    ports:
      - "5432:5432"

#OBS: Sabemos que por boas práticas de segurança, o usuário master do banco não deve ser usado pela APP.
Porém trata-se de uma demonstração...
```

Após isso, foi feito o restore do northwind.sql fornecido pelo desafio em questão, foi criada a tabela order_details através do csv também fornecido!
Comandos: 
```
restore dump DB northwind
psql -U airflow -h localhost -d northwind -f northwind.sql

create table order_details
CREATE TABLE order_details (
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    unit_price INTEGER,
    quantity INTEGER,
    discount INTEGER,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

comando de COPY csv para a tabela order_details
COPY order_details(order_id, product_id, unit_price, quantity, discount)
FROM '/tmp/order_details.csv'
DELIMITER ','
CSV HEADER;
```

#Instalando e executando Airflow via python3 -m venv
Após instalar e iniciar o airflow, criei 3 DAGS:

## DAG 01: dag_tranformation_db
dag responsável por limpar meu banco de dados northwind_datamart (datamart porque ele foi criado pós (ET-(L)),  ela também faz a criação de todas as tabelas pós drop/create database
 ```
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import psycopg2
from psycopg2 import sql

db_config = {'host': 'localhost','database': 'northwind','user': 'airflow','password': 'passairflow','database_new': 'northwind_datamart'}
os.environ['PGPASSWORD'] = 'passairflow'

# DDL das tabelas
tables = {
    'categories': """
        CREATE TABLE IF NOT EXISTS categories (
            category_id smallint NOT NULL,
            category_name character varying(15) NOT NULL,
            description text,
            picture bytea
        );
    """,
    'products': """
        CREATE TABLE IF NOT EXISTS products (
            product_id SERIAL PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            supplier_id INTEGER,
            category_id INTEGER,
            quantity_per_unit VARCHAR(255),
            unit_price NUMERIC,
            units_in_stock INTEGER,
            units_on_order INTEGER,
            reorder_level INTEGER,
            discontinued BOOLEAN
        );
    """,
    # Adicione as outras tabelas conforme necessário
    'suppliers': """
        CREATE TABLE IF NOT EXISTS suppliers (
            supplier_id SERIAL PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL,
            contact_name VARCHAR(255),
            contact_title VARCHAR(255),
            address TEXT,
            city VARCHAR(255),
            region VARCHAR(255),
            postal_code VARCHAR(20),
            country VARCHAR(255),
            phone VARCHAR(50),
            fax VARCHAR(50),
            homepage TEXT
        );
    """,
    'employees': """
        CREATE TABLE IF NOT EXISTS employees (
            employee_id smallint NOT NULL,
            last_name character varying(20) NOT NULL,
            first_name character varying(10) NOT NULL,
            title character varying(30),
            title_of_courtesy character varying(25),
            birth_date date,
            hire_date date,
            address character varying(60),
            city character varying(15),
            region character varying(15),
            postal_code character varying(10),
            country character varying(15),
            home_phone character varying(24),
            extension character varying(4),
            photo bytea,
            notes text,
            reports_to float,
            photo_path character varying(255)
        );
    """,
    'employee_territories': """
        CREATE TABLE IF NOT EXISTS employee_territories (
            employee_id INTEGER,
            territory_id VARCHAR(255),
            PRIMARY KEY (employee_id, territory_id)
        );
    """,
    'order_details': """
        CREATE TABLE IF NOT EXISTS order_details (
            order_id INTEGER,
            product_id INTEGER,
            unit_price NUMERIC,
            quantity INTEGER,
            discount NUMERIC,
            PRIMARY KEY (order_id, product_id)
        );
    """,
    'orders': """
        CREATE TABLE IF NOT EXISTS orders (
            order_id SERIAL PRIMARY KEY,
            customer_id VARCHAR(255),
            employee_id INTEGER,
            order_date DATE,
            required_date DATE,
            shipped_date DATE,
            ship_via INTEGER,
            freight NUMERIC,
            ship_name VARCHAR(255),
            ship_address TEXT,
            ship_city VARCHAR(255),
            ship_region VARCHAR(255),
            ship_postal_code VARCHAR(20),
            ship_country VARCHAR(255)
        );
    """,
    'customers': """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(255) PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL,
            contact_name VARCHAR(255),
            contact_title VARCHAR(255),
            address TEXT,
            city VARCHAR(255),
            region VARCHAR(255),
            postal_code VARCHAR(20),
            country VARCHAR(255),
            phone VARCHAR(50),
            fax VARCHAR(50)
        );
    """,
    'customer_customer_demo': """
        CREATE TABLE IF NOT EXISTS customer_customer_demo (
            customer_id VARCHAR(255),
            customer_type_id VARCHAR(255),
            PRIMARY KEY (customer_id, customer_type_id)
        );
    """,
    'customer_demographics': """
        CREATE TABLE IF NOT EXISTS customer_demographics (
            customer_type_id VARCHAR(255) PRIMARY KEY,
            customer_desc TEXT
        );
    """,
    'shippers': """
        CREATE TABLE IF NOT EXISTS shippers (
            shipper_id SERIAL PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL,
            phone VARCHAR(50)
        );
    """,
    'us_states': """
        CREATE TABLE IF NOT EXISTS us_states (
            state_id SERIAL PRIMARY KEY,
            state_name VARCHAR(255),
            state_abbr VARCHAR(2),
            state_region VARCHAR(255)
        );
    """
}

#conectando no PSQL OLD
def connect_db():
    conn = psycopg2.connect(host=db_config['host'],dbname=db_config['database'],user=db_config['user'])
    return conn

#Deletando o datamart 
def drop_database():
    conn = connect_db()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_config['database_new']}'")
    exists = cursor.fetchone()
    if exists:
        cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_config['database_new'])))
    cursor.close()
    conn.close()

#Criando o novo datamart limpo
def create_database():
    conn = connect_db()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_config['database_new']}'")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_config['database_new'])))
    cursor.close()
    conn.close()

#conectando no PSQL
def connect_db_new():
    conn = psycopg2.connect(host=db_config['host'],dbname=db_config['database_new'],user=db_config['user'])
    return conn

# Função para apagar e recriar as tabelas
def drop_and_create_tables():
    conn = connect_db_new()
    cursor = conn.cursor()
    for table, create_table_sql in tables.items():
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table)))
        cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print("Tabelas foram limpas e recriadas com sucesso.")

# Definindo argumentos padrão para a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definindo a DAG
dag = DAG(
    'dag_transformation_db',
    default_args=default_args,
    description='Uma DAG para criar tabelas no banco de dados',
    schedule_interval=None,  # Define para None para evitar execução contínua
)

# Definindo as tarefas
create_db_task = PythonOperator(
    task_id='create_db_task',
    python_callable=create_database,
    dag=dag,
)

create_tables_task = PythonOperator(
    task_id='create_tables_task',
    python_callable=drop_and_create_tables,
    dag=dag,
)

# Definindo a ordem das tarefas
create_db_task >> create_tables_task
```

## DAG 02: dag_export_db_to_csv
Responsável por exportar todas as tabelas do banco northwind e gravar no seguinte formato: YYYY-MM-DD/csv/tablenames.csv no destino /tmp/
```
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
```
## DAG 03: dag_import_csv_to_new_db
Responsável por importar as tabelas armazenadas no /csv/tablesnames.csv para o novo banco de dados chamado northwind_datamart. Conforme solicitado, ela espera uma variável IMPORT_FRON_DATE com o valor do dia que deseja importar para a base, ser preenchida nesse formato: YYYY-MM-DD, caso vazia, o sript pega a data do dia atual.
```
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
```

# PRINTs apresentando algumas ETAPAS do code-challenge
![image](https://github.com/everton-amaral/code-challenge/assets/38400444/59a27063-d379-4d47-a81e-14d065f8d09f)

Criando a tabela e importando os dados do CSV:

![image](https://github.com/everton-amaral/code-challenge/assets/38400444/11f1f822-e575-44cd-bd70-d924539d1d81)

# DAG 01:
<img width="1490" alt="image" src="https://github.com/everton-amaral/code-challenge/assets/38400444/60df9cba-3384-4b25-9431-5ed74b3e8afc">

![image](https://github.com/everton-amaral/code-challenge/assets/38400444/3a6d9938-c080-46f0-8cd1-5acd1700a711)

# DAG 02
<img width="1491" alt="image" src="https://github.com/everton-amaral/code-challenge/assets/38400444/d5f34da9-4160-49fc-bf38-aaadc40e027f">








