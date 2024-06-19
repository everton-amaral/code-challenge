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

Após isso, foi feito o restore do northwind.sql fornecido pelo desafio em questão, também foi criada a tabela order_details através do csv também fornecido!
