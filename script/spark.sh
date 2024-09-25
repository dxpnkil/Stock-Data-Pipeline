docker cp spark/calculate_return.py  spark-master:calculate_return.py
docker compose exec spark-master /spark/bin/spark-submit calculate_return.py