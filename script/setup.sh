#!/bin/bash

bash script/superset_init.sh
bash script/put_data_into_hdfs.sh
bash script/hive.sh
bash script/spark.sh
bash script/flink.sh