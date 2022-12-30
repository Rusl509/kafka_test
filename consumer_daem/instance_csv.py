import pandas as pd
import psycopg2


import io
from loguru import logger
import sys
import os
from dotenv import load_dotenv


load_dotenv()
server = os.getenv("SERVER_GREEN")
database = os.getenv("DATABASE")
password = os.getenv("PASSWORD")
login = os.getenv("LOGIN")


logger.add(sys.stdout, format="{time} {level} {message}", level= "INFO")


def write_in_gp(list_of_messages, records):
    """Normalize the batch of messages and translate it into a dataframe, then insert it into the stg table"""
    dataframe = pd.json_normalize(list_of_messages)
    csv_io = io.StringIO() # creating a stream
    dataframe.to_csv(csv_io, sep='\t', header=False, index=False)
# Moving the file pointer to read
    csv_io.seek(0)
# Connect to the GreenPlum database.
    greenplum = psycopg2.connect(host= "busy_burnell", database= "Rus", user= "gpadmin", password= "dataroad")
    gp_cursor = greenplum.cursor()
    logger.info("Greenplum connection was opened")
# Copy the data from the buffer to the table.
    gp_cursor.copy_from(csv_io, 'stg')
    greenplum.commit()
    logger.info(f"{records} records were recorded")
# Close the GreenPlum cursor and connection.
    gp_cursor.close()
    greenplum.close()
    logger.info("connection was closed")