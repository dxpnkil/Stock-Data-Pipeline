docker compose exec hive-server hiveserver2
docker cp script/init.sql  hive-server:init.sql
docker compose exec hive-server hive -f /init.sql