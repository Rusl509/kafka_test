from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from datetime import datetime
from typing import Dict
import sys
import logging
from logging import StreamHandler, Formatter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = StreamHandler(stream = sys.stdout)
handler.setFormatter(Formatter(fmt = '[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)



def select_and_insert(task_instance):
    pg_hook = PostgresHook(postgres_conn_id = "greenplum")
    try:
        df = pg_hook.get_pandas_df(sql="SELECT * FROM stg")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        if df.empty:
            return logger.info("no data available")
        count_of_recordes = pg_hook.get_pandas_df(sql="select count(id_sensor) FROM dds")
        count_of_recordes = count_of_recordes.iloc[0]["count"]
        logger.info(f"{count_of_recordes} records were read")
        cursor.execute("TRUNCATE TABLE stg")
        logger.info(f"{count_of_recordes} records were deleted")
        connection.commit()
        df_negative_temperature = df[df['temperature']<0]
        columns = list(df)


        if df_negative_temperature.empty:
            return logger.info("no data with negative temperature")
        count_of_recordes_dds = pg_hook.get_pandas_df(sql="select count(id_sensor) FROM dds")
        count_of_recordes_dds = count_of_recordes_dds.iloc[0]["count"]
        value = df_negative_temperature.values
        pg_hook.insert_rows('dds', value, columns)
        logger.info(f"{count_of_recordes} records were recorded in the dds layer")


    except Exception as er:
        logger.exception("exception")


    finally:
        cursor.close()
        connection.close()
    # path = "qwee.csv"
    # df.to_csv(path)
    # task_instance.xcom_push(key="path", value=path)





# def deletee():
#     pg_hook = PostgresHook(postgres_conn_id = 'greenplum')
#     connection = pg_hook.get_conn()
#     cursor = connection.cursor()
#     cursor.execute("TRUNCATE TABLE test")
#     connection.commit()


# # def write(task_instance):
# #     buffer = StringIO()
# #     path = task_instance.xcom_pull(key="path")
# #     df = pd.read_feather(path)
# #     print('1231111')
# #     print(df)
# #     buffer.seek(0)
# #     pg_hook = PostgresHook(postgres_conn_id = 'greenplum')
# #     conn = pg_hook.get_conn()
# #     cursor = conn.cursor()
# #     try:
# #         cursor.copy_from(buffer, "dds", sep=",")
# #         conn.commit()
# #     except Exception as error:
# #         print("Error: %s" % error)
# #         conn.rollback()
# #         cursor.close()
# #         return 1
# #     print("copy_from_stringio() done")
# #     cursor.close()

# #     # pg_hook.insert_rows("dds", df)
# #     os.remove(path)


# def execute_mogrify(task_instance):
#     """
#     Using cursor.mogrify() to build the bulk insert query
#     then cursor.execute() to execute the query
#     """
#     # Create a list of tupples from the dataframe values
#     path = task_instance.xcom_pull(key="path")
#     df = pd.read_csv(path)
#     print(df)
#     df = df.drop(columns='Unnamed: 0') 
#     tuples = [tuple(x) for x in df.to_numpy()]
#     # Comma-separated dataframe columns
#     cols = ','.join(list(df.columns))
#     # SQL quert to execute
#     pg_hook = PostgresHook(postgres_conn_id = 'greenplum://gpadmin:dataroad@localhost/Rus')
#     conn = pg_hook.get_conn()
#     cursor = conn.cursor()
#     values = [cursor.mogrify("(%s,%s)", tup).decode('utf8') for tup in tuples]
#     query  = "INSERT INTO %s(%s) VALUES " % ("dds", cols) + ",".join(values)
#     try:
#         cursor.execute(query, tuples)
#         conn.commit()
#     except Exception as error:
#         print("Error: %s" % error)
#         conn.rollback()
#         cursor.close()
#         return 1
#     print("execute_mogrify() done")
#     cursor.close()
#     os.remove(path)



with DAG('read_and_insert_dds', schedule_interval='*/1 * * * *', start_date=datetime(2022, 12, 1), catchup=False) as dag:
    
    extract_data_and_insert = PythonOperator(
        task_id='extract_and_insert',
        python_callable=select_and_insert
    )

    # process_data = PythonOperator(
    #     task_id='process_data',
    #     python_callable=deletee
    # )

    # store_data = PythonOperator(
    #     task_id='store_data',
    #     python_callable=execute_mogrify
    # )

    extract_data_and_insert 

