import logging
import boto3
from datetime import datetime, timedelta
import dateutil.tz
import logging
import logging.config
import traceback
import sys
import psycopg2
from dotenv import load_dotenv
import os
import pytz

# Carga las variables de entorno desde el archivo .env
load_dotenv()

#Nos ubicamos en el directorio de trabajo
os.chdir('/opt/custom_scripts_monitoring/aws_performance_insights/')

postgres_pass=os.getenv('POSTGRES_PASSWORD')
postgres_user=os.getenv('POSTGRES_USER')
postgres_db=os.getenv('POSTGRES_DATABASE')
host_db=os.getenv('POSTGRES_HOST')

logging.config.fileConfig(f'export_pi_metrics_logging.conf')
logging.getLogger('export_db_pi_metrics').setLevel(logging.WARNING)

# Conexión a la base de datos
conn = psycopg2.connect(host=host_db, database=postgres_db, user=postgres_user, password=postgres_pass, port="5432")

# Crear cursor
cur = conn.cursor()

## Creamos la tabla si no existe
cur.execute('''
                CREATE TABLE IF NOT EXISTS rds_pi_db_metrics (
                    rds_name TEXT,
                    dbinstanceidentifier TEXT,
                    metric TEXT,
                    metric_timestamp TIMESTAMP,
                    value FLOAT,
                    UNIQUE(dbinstanceidentifier,metric_timestamp,metric)
                )
            ''')


role_arn='your_arn_role'
region_name='eu-west-1'
sts_connection = boto3.client('sts')

try:
    acct_b = sts_connection.assume_role(
        RoleArn=role_arn,
        RoleSessionName="cross_acct_rds_pi"
    )
except Exception as e:
    #print(f'Exception raised: {traceback.format_exc()}')
    logging.getLogger('export_db_pi_metrics').error(f'Exception raised: {traceback.format_exc()}')

ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
SECRET_KEY = acct_b['Credentials']['SecretAccessKey']
SESSION_TOKEN = acct_b['Credentials']['SessionToken']

pi=boto3.client('pi',region_name=region_name,aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,aws_session_token=SESSION_TOKEN)
rds=boto3.client('rds',region_name=region_name,aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,aws_session_token=SESSION_TOKEN)
rds_details=rds.describe_db_instances()

marker = None
rds_list=[]
paginator = rds.get_paginator('describe_db_instances')

while True:
    paginator = rds.get_paginator('describe_db_instances')
    response_iterator = paginator.paginate(
        PaginationConfig={
            'StartingToken': marker})
    for page in response_iterator:
        for instance in page.get('DBInstances'):
            if instance.get('PerformanceInsightsEnabled') and instance.get('Engine') == 'aurora-postgresql':
                rds_list.append(instance.get('DBInstanceIdentifier'))
    try:
        marker = page['NextToken']
    except KeyError:
        break

logging.getLogger('export_db_pi_metrics').info(f"Extrayendo datos de performance insishgt para: {rds_list}")

#Declaramos la zona horaria de madrid
madrid_tz = pytz.timezone('Europe/Madrid')

##Recorremos las bases de datos para sacar las queries
for rds_name in rds_list:
    rds_id=rds.describe_db_instances(DBInstanceIdentifier=rds_name)['DBInstances'][0]['DbiResourceId']
    ###Consulta con zona horario utc para interactuar con
    start_time=datetime.now()-timedelta(minutes=5)
    end_time=datetime.now()
    try:
        response = pi.get_resource_metrics(
            ServiceType='RDS',
            Identifier=rds_id,
            StartTime=start_time,
            EndTime=end_time,
            PeriodInSeconds=60,
            MetricQueries=[
                {
                    'Metric': 'db.state.idle_in_transaction_count.avg',
                },
                {
                    'Metric': 'db.state.idle_in_transaction_aborted_count.avg',
                },
                {
                    'Metric': 'db.Transactions.active_transactions.avg',
                },
                {
                    'Metric': 'db.Transactions.blocked_transactions.avg',
                },
                {
                    'Metric': 'db.SQL.queries_started.avg',
                },
                {
                    'Metric': 'db.SQL.queries_finished.avg',
                },
                {
                    'Metric': 'db.SQL.total_query_time.avg',
                },
                {
                    'Metric': 'db.Concurrency.deadlocks.avg',
                },
            ],
        )
        logging.getLogger('export_db_pi_metrics').info(f"Metricas extraidas para {rds_name}: {response}")
    except Exception as e:
        logging.getLogger('export_db_pi_metrics').error(f'Exception devuelta extrayendo metricas de PI: {traceback.format_exc()}')
    for metric in response['MetricList']:
        for datapoint in metric['DataPoints']:
            logging.getLogger('export_db_pi_metrics').info(f"{rds_name}-{response['Identifier']}-{metric['Key']['Metric']}-{datapoint['Timestamp']}-{datapoint.get('Value')}")
            cur.execute('''
                INSERT INTO rds_pi_db_metrics (rds_name, dbinstanceidentifier, metric, metric_timestamp, value)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (dbinstanceidentifier,metric_timestamp,metric) DO NOTHING
            ''', (
                rds_name,
                response['Identifier'],
                metric['Key']['Metric'],
                datapoint['Timestamp'].astimezone(madrid_tz),
                datapoint.get('Value')
            ))

try:
    # Delete old entries
    cur.execute("DELETE FROM rds_pi_db_metrics WHERE metric_timestamp < now() - INTERVAL '2 weeks';")
except Exception as e:
    #print(f'Exception raised: {traceback.format_exc()}')
    logging.getLogger('export_db_pi_metrics').error(f'Exception raised: {traceback.format_exc()}')

#Commit changes and close connection
conn.commit()
cur.close()
conn.close()
