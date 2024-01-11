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

# Carga las variables de entorno desde el archivo .env
load_dotenv()

#Nos ubicamos en el directorio de trabajo
os.chdir('/opt/custom_scripts_monitoring/aws_performance_insights/')

postgres_pass=os.getenv('POSTGRES_PASSWORD')
postgres_user=os.getenv('POSTGRES_USER')
postgres_db=os.getenv('POSTGRES_DATABASE')
host_db=os.getenv('POSTGRES_HOST')

logging.config.fileConfig(f'export_pi_metrics_logging.conf')
logging.getLogger('export_pi_metrics').setLevel(logging.WARNING)

# Conexión a la base de datos
conn = psycopg2.connect(host=host_db, database=postgres_db, user=postgres_user, password=postgres_pass, port="5432")

# Crear cursor
cur = conn.cursor()

## Creamos la tabla si no existe
# Crear tabla
cur.execute("""CREATE TABLE IF NOT EXISTS rds_pi_metrics (
                fecha_consulta timestamp,
                fecha_inicio timestamp,
                fecha_fin timestamp,
                rds varchar,
                rds_instance_identifier varchar,
                query_group varchar,
                item varchar,
                load_cpu float,
                UNIQUE(fecha_consulta,rds)
            )""")

#tz = dateutil.tz.gettz('Europe/Madrid')
now = datetime.now()
#print(datetime.now(tz=tz))

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
    logging.getLogger('export_pi_metrics').error(f'Exception raised: {traceback.format_exc()}')

ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
SECRET_KEY = acct_b['Credentials']['SecretAccessKey']
SESSION_TOKEN = acct_b['Credentials']['SessionToken']

performance_insights=boto3.client('pi',region_name=region_name,aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,aws_session_token=SESSION_TOKEN)
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
            if instance.get('PerformanceInsightsEnabled') is True:
                rds_list.append(instance.get('DBInstanceIdentifier'))
    try:
        marker = page['NextToken']
    except KeyError:
        break

logging.getLogger('export_pi_metrics').info(f"Extrayendo datos de performance insishgt para: {rds_list}")

##Recorremos las bases de datos para sacar las queries
for rds_name in rds_list:
    rds_id=rds.describe_db_instances(DBInstanceIdentifier=rds_name)['DBInstances'][0]['DbiResourceId']
    start_date=now-timedelta(minutes=5)
    end_date=now
    try:
        top_sql=performance_insights.get_resource_metrics(
            ServiceType='RDS',
            Identifier=rds_id,
            StartTime=start_date,
            EndTime=end_date,
            PeriodInSeconds=60,
            MetricQueries=[{"Metric": "db.load.avg","GroupBy": { "Group": "db.sql_tokenized", "Limit": 10 }},{"Metric": "db.load.avg","GroupBy": { "Group": "db.user", "Limit": 10 }},{"Metric": "db.load.avg","GroupBy": { "Group": "db.wait_event", "Limit": 10 }} ]
        )
        logging.getLogger('export_pi_metrics').info(f"Metricas extraidas para {rds_name}: {top_sql}")
    except Exception as e:
        logging.getLogger('export_pi_metrics').error(f'Exception raised: {traceback.format_exc()}')

    start_date=datetime.now().replace(tzinfo=None)-timedelta(minutes=5)
    end_date=datetime.now().replace(tzinfo=None)

    for query in top_sql['MetricList']:
        try:
            insert_data=""
            data_points=query.get('DataPoints',[])
            value_avg=0
            total_items=0
            for item in data_points:
                value_avg+=item.get('Value',0)
                total_items+=1
            value_avg=value_avg/total_items
            value_avg=round(value_avg,4)
            if query.get('Key',{}).get('Dimensions',{}).get('db.sql_tokenized.statement') != None:
                fecha_consulta=datetime.now().replace(tzinfo=None)
                query_group="db.sql_tokenized"
                item=query.get('Key',{}).get('Dimensions',{}).get('db.sql_tokenized.statement')
                cur.execute("INSERT INTO rds_pi_metrics VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (fecha_consulta, start_date, end_date, rds_name, rds_id, query_group, item, value_avg))
            if query.get('Key',{}).get('Dimensions',{}).get('db.user.name') != None:
                fecha_consulta=datetime.now().replace(tzinfo=None)
                query_group="db.user.name"
                item=query.get('Key',{}).get('Dimensions',{}).get('db.user.name')
                cur.execute("INSERT INTO rds_pi_metrics VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (fecha_consulta, start_date, end_date, rds_name, rds_id, query_group, item, value_avg))
            if query.get('Key',{}).get('Dimensions',{}).get('db.wait_event.name') != None:
                fecha_consulta=datetime.now().replace(tzinfo=None)
                query_group="db.wait_event.name"
                item=query.get('Key',{}).get('Dimensions',{}).get('db.wait_event.name')
                cur.execute("INSERT INTO rds_pi_metrics VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (fecha_consulta, start_date, end_date, rds_name, rds_id, query_group, item, value_avg))
        except Exception as err:
            print(f'Exception raised: {traceback.format_exc()}')

try:
    # Delete old entries
    cur.execute("DELETE FROM rds_pi_metrics WHERE fecha_consulta < NOW() - INTERVAL '1 week';")
except Exception as e:
    logging.getLogger('export_pi_metrics').error(f'Exception raised: {traceback.format_exc()}')

#Commit changes and close connection
conn.commit()
cur.close()
conn.close()
