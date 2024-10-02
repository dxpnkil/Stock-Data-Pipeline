docker compose exec kafka kafka-topics.sh --create --topic stock --replication-factor 1 --partitions 3 --bootstrap-server kafka:9092
docker compose exec kafka kafka-console-consumer.sh --topic stock  --bootstrap-server kafka:9092
docker compose exec kafka kafka-topics.sh --list   --bootstrap-server kafka:9092
kafka-topics.sh --list --bootstrap-server kafka:9092
docker compose exec kafka kafka-console-producer.sh --topic stock  --bootstrap-server kafka:9092
# docker compose exec kafka1 /bin/kafka-topics --describe --topic stock  --bootstrap-server kafka:9092

{"ticker": "FPT", "date": "2024-09-27", "open": 150.0, "high": 155.0, "low": 149.0, "close": 154.0, "volume": 1000000}


