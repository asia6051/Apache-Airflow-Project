# Apache Airflow - Joanna Piwonska

## Description
This project contains a few directories:
1. config
    - with airflow.cfg file
2. dags
    - with 3 DAGs: trigger_dag and jobs_dag that were expanded with every module of ther course and Mod19_dag that contains quick task from module 19 about ETL pipelines.
3. logs
4. other_files
    - with csv file required for Mod19_dag
5. plugins
    - with one custom operator and one custom sensor required in project

and ```docker-compose.yaml```file.

## Installation
To set up the project you need to use [Docker Engine](https://docs.docker.com/engine/install/) with attached docker-compose.yaml file. After you use docker-compose up command in project folder you should wait a minute or two to let everything set up properly. To use Airflow UI you need to open http://localhost:8080/ in your browser and log in with user: airflow password: airflow.

## Usage
Project contains 3 DAGs

### JOBS_DAG
1. print info about proccess start
2. get current user info
3. check if table_1 exists
4. branch task that chooses next step based on the fact if table_1 exists or not
   - NO - ```create table_1```
   - YES - ```insert row(custom_id, user_name, timestamp) into table_1```
5. query table_1 using custom PostgreSQLCountRowsOperator and print number of rows
   
### TRIGGER_DAG

**Tu use this dag successfully you need to get through this 2 steps:**
   1. Get in airflow worker container and create test.txt file: 
   ```
   docker exec -it <worker_docker_id> /bin/bash 
   touch /tmp/test.txt
   ```
   2. Create vault secret with slack token:
   ```
   docker exec -it <vault_docker_id> sh
   vault login `vault_login`
   vault secrets enable -path=airflow -version=2 kv
   vault kv put airflow/variables/slack_token value=`slack_token`
   ```


**Now you can trigger the trigger_dag which will:**
1. wait for test.txt file to appear in /tmp path
2. trigger jobs_dag and wait for it to finish
3. print info about triggered jobs_dag instance
4. remove test.txt file
5. create timestamp file at /tmp path
6. alert Slack with message containing DAG ID and execution date


### MOD19_DAG
1. download data from ```/other_files/monroe-county-crash.csv``` and insert them into postgreSQL database
2. query data to see accidents count per year
3. print query results
      
