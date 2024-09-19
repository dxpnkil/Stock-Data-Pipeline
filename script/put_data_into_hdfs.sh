docker cp ./data/stock_data.csv namenode:stock_data.csv
docker compose exec namenode hdfs dfs -mkdir -p /data/stock
docker compose exec namenode hdfs dfs -put -f stock_data.csv /data/stock/stock_data.csv