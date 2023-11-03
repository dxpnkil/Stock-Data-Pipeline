from kafka import KafkaConsumer


consumer = KafkaConsumer(
            'stockData',
            bootstrap_servers=['localhost:29092', 'localhost:19092', 'localhost:39092'],
            group_id='stockDataConsummers',
            auto_offset_reset='earliest',
            enable_auto_commit=False)

try:
    print("Subcribe to topic stockData")
    while True:
        msgs_pack = consumer.poll(10.0)
        if msgs_pack is None:
            continue

        for tp, messages in msgs_pack.items():
            for message in messages:
                true_msg = str(message[6])[2: len(str(message[6])) - 1]
                print(true_msg)

except Exception as e:
    print(
        f"An error happened while processing messages from kafka: {e}")
finally:
    consumer.close()