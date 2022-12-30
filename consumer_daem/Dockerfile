FROM ubuntu


RUN apt update
RUN apt install python3 python3-pip -y


RUN pip3 install loguru
RUN pip3 install python-daemon
RUN pip3 install kafka-python
RUN pip3 install python-dotenv
RUN pip3 install pandas
RUN pip3 install psycopg2-binary

COPY . /consumer

CMD [ "python3", "/consumer/daem_consumer.py" ]