FROM ubuntu


RUN apt update
RUN apt install python3 python3-pip -y


RUN pip3 install loguru
RUN pip3 install python-daemon
RUN pip3 install kafka-python
RUN pip3 install python-dotenv

COPY . /producer

CMD [ "python3", "/producer/producer_daem.py" ]