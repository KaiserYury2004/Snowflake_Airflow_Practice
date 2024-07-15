import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
import snowflake.connector

# Создание DAG
dag = DAG(
    'snowflake_dag',
    schedule_interval=None
)
# Путь к локальному файлу(напоминаю-для WSL путь указывается особым способом)
local_file_path = '/path/to/your/file/Airline_Dataset.csv'

file_format_name = 'my_csv_format'

# Функция для загрузки файла в Snowflake Stage
def upload_file_to_snowflake_stage():
    # Подключение к Snowflake через Airflow Connection
    conn = BaseHook.get_connection('snowflake_conn')
    conn_params = {
        'user': conn.login,
        'password': conn.password,
        'account': conn.extra_dejson.get('account'),
        'warehouse': conn.extra_dejson.get('warehouse'),
        'database': conn.extra_dejson.get('database'),
        'schema': conn.extra_dejson.get('schema')
    }
    con = snowflake.connector.connect(**conn_params)
    cur = con.cursor()
    # Команды для создания Stage и загрузки файла
    cur.execute(f"PUT file://{local_file_path} @~/staged_files AUTO_COMPRESS=TRUE")
    cur.close()
    con.close()

# Задача для загрузки файла в Snowflake Stage
upload_to_stage = PythonOperator(
    task_id='Upload_to_Stage',
    python_callable=upload_file_to_snowflake_stage,
    dag=dag,
)

# Задача для создания формата файла в Snowflake
create_file_format = SnowflakeOperator(
    task_id='Create_File_format',
    sql=f"""
    CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;
    """,
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)
## Копирование файла в Stage
Stage_1 = SnowflakeOperator(
    task_id='Copy_to_raw_data',
    sql=f"""
    COPY INTO raw_data
    FROM @~/staged_files
    FILE_FORMAT = (FORMAT_NAME = '{file_format_name}');
    """,
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)
# Задача для трансформации данных из raw_data в clean_data
Stage_2 = SnowflakeOperator(
    task_id='Transform_data',
    sql="""
    INSERT INTO clean_data
    SELECT * FROM raw_data
    """,
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)
## удаление колонки позиции в списке
Stage_3 = SnowflakeOperator(
    task_id='Transformation_of_clean',
    sql="""
    ALTER TABLE RAW.clean_data DROP COLUMN Position;
    """,
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)

# Задача для загрузки данных в datamart_data
Stage_4 = SnowflakeOperator(
    task_id='Load_to_datamart',
    sql="""
    Insert into datamart_data
    SELECT Passenger_Id,First_Name ,Last_Name,Gender ,Age ,Nationality ,Airport_Name ,Airport_Country_Code ,Country_Name ,Airport_Continent ,
        Continents ,To_DATE(Departure_time),Arrival_Airport,Pilot_Name ,Flight_Status ,Ticket_Type ,Passenger_Status 
    FROM clean_data
    """,
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)

# Задача для логирования количества обработанных строк
log_audit = SnowflakeOperator(
    task_id='Log_audit',
    sql="""
    INSERT INTO audit_log (operation, affected_rows, timestamp)
    SELECT 'INSERT', COUNT(*), CURRENT_TIMESTAMP FROM raw_data;
    """,
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)
#Порядок выполнения задач
upload_to_stage >> create_file_format >> Stage_1 >> Stage_2 >> Stage_3 >>Stage_4>> log_audit
