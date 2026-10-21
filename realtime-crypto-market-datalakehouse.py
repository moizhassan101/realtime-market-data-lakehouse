# Databricks notebook source
namespace = "RealTimeMarketDataLakehouse"
connection_string = dbutils.secrets.get(scope="market-lakehouse", key="eventhub-conn-str")

kafka_options = {
    "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{connection_string}";',
    "subscribe":"crypto-prices",
    "startingOffsets": "earliest"
}

raw_df = spark.readStream.format("kafka").options(**kafka_options).load()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS workspace;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.default;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.checkpoints;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from delta.tables import DeltaTable

# 1. Parse JSON payload
schema = StructType([
    StructField("coin", StringType()),
    StructField("usd", DoubleType()),
    StructField("timestamp", LongType())
])

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 2. Create the target delta table (once)
spark.sql("""
          CREATE TABLE IF NOT EXISTS workspace.default.crypto_prices_delta (
              coin STRING, usd DOUBLE, timestamp LONG
          ) USING DELTA
          """)

# 3. Upsert logic - runs per micro batch
def upsert_to_delta(batch_df, batch_id):
    window = Window.partitionBy("coin").orderBy(F.col("timestamp").desc())
    deduped_df = (batch_df
                  .withColumn("rn", F.row_number().over(window))
                  .filter("rn = 1")
                  .drop("rn")
                  )

    target = DeltaTable.forName(spark, "workspace.default.crypto_prices_delta")
    (target.alias("t")
     .merge(deduped_df.alias("s"), "t.coin = s.coin")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    
# 4. Strat the stream
query = (parsed_df.writeStream
         .foreachBatch(upsert_to_delta)
         .option("checkpointLocation", "/Volumes/workspace/default/checkpoints/crypto_prices_merge_v5")
         .trigger(availableNow=True)
         .start()
         )

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.crypto_prices_delta;

# COMMAND ----------

