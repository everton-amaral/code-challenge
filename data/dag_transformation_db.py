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