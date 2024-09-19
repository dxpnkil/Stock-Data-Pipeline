from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, log, exp, sum
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Returns Calculation") \
    .enableHiveSupport() \
    .getOrCreate()
    
spark.sql("use stock")

df = spark.sql("SELECT * FROM daily_dim ORDER BY date")

window_spec = Window.partitionBy("ticker").orderBy("date")

df = df.withColumn("lag_close", lag("close", 1).over(window_spec))

df = df.filter(col("lag_close").isNotNull())

df = df.withColumn("simple_return", (col("close") / col("lag_close")) - 1)

df = df.withColumn("log_return", log(col("close") / col("lag_close")))

df = df.withColumn("cum_log_return", sum("log_return").over(window_spec))

df = df.withColumn("cumulative_return", exp(col("cum_log_return")))

df = df.filter(col("ticker").isin("FPT", "VCB", "BID", 'MWG'))

df = df.select('ticker', 'date', 'simple_return', 'log_return', 'cumulative_return')

df.write \
    .mode("overwrite") \
    .saveAsTable("stock_returns")
