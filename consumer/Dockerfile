FROM python:alpine

WORKDIR /app

RUN python -m pip install pika

RUN python -m pip install influxdb-client

COPY . .

CMD ["python", "listener.py"]