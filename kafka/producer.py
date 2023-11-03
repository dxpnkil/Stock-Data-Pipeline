from kafka import KafkaProducer
from kafka.errors import KafkaError

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092', 'localhost:19092', 'localhost:39092'],security_protocol="PLAINTEXT",
        client_id='producer')
    stock_info = "2212,21,2,3,4"
    producer.send('stockData', bytes(
                        stock_info, encoding='utf-8'))
    producer.flush()
except KafkaError as e:
    print(f"An Kafka error happened: {e}")
except Exception as e:
    print(
        f"An error happened while pushing message to Kafka: {e}")