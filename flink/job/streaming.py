from pyflink.table.catalog import HiveCatalog

from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

catalog_name = "myhive"
hive_conf_dir = "/opt/hive-conf"
database = "stock"

jar_files = "file:///jar/flink-connector-hive_2.11-1.14.5.jar;file:///jar/hive-exec-2.3.2.jar;file:///jar/flink-sql-connector-hive-2.3.6_2.11-1.14.5.jar;file:///jar/flink-sql-connector-kafka_2.11-1.14.5.jar"


env = StreamExecutionEnvironment.get_execution_environment()
settings = (
    EnvironmentSettings.new_instance().use_blink_planner().in_streaming_mode().build()
)
table_env = StreamTableEnvironment.create(env, environment_settings=settings)
table_env.get_config().get_configuration().set_string("pipeline.jars", jar_files)

deserialization_schema = (
    JsonRowDeserializationSchema.builder()
    .type_info(
        type_info=Types.ROW_NAMED(
            ["ticker", "date", "open", "high", "low", "close", "volume"],
            [
                Types.STRING(),
                Types.STRING(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.INT(),
            ],
        )
    )
    .build()
)

kafka_consumer = FlinkKafkaConsumer(
    topics="stock",
    deserialization_schema=deserialization_schema,
    properties={"bootstrap.servers": "kafka:9092", "group.id": "stock"},
)

ds = env.add_source(kafka_consumer)

hive_catalog = HiveCatalog(catalog_name, database, hive_conf_dir)
table_env.register_catalog(catalog_name, hive_catalog)
table_env.use_catalog(catalog_name)
table_env.use_database(database)

table_env.create_temporary_view(
    "source_table", ds, "ticker, date, open, high, low, close, volume"
)
table_env.execute_sql(
    """
    INSERT INTO daily_dim
    SELECT ticker, CAST(`date` AS DATE) as `date`, `open`, high, low, `close`, volume  FROM source_table
"""
).print()

env.execute("Kafka to Hive Job")
