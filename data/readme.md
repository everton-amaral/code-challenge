# iniciando processo de engenharia de dados (ETL)

## Ferramentas utilizadas: Docker, PostgreSQL, python3 c/ pandas e Airflow

Após realizar a instalação do docker, foi criado um banco de dados PostgreSQL na versão 16.

docker-compose.yaml
version: '3.8'

services:
  db:
    image: postgres:latest
    volumes:
      - /Users/carolrmr/postgresql/data
    environment:
      POSTGRES_DB: postgres
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: passairflow
    ports:
      - "5432:5432"
Sabemos que por boas práticas de segurança, o usuário master do banco não deve ser usado pela APP. Porém trata-se de uma demonstração...
