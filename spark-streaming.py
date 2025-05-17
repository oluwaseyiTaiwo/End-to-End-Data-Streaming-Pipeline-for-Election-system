from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()


if __name__ == "__main__":
    #print(pyspark.__version__) #3.5.5
    spark = (
    SparkSession.builder
      .appName("Data_Stream_pipeline")
      .master("local[*]")
       .config(
          "spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,""com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.0")
      .config("spark.jars",r"C:\Users\oluwa\Desktop\Project\End-to-End Data-Pipeline-for-Election-Voting-system-kafka-spark-postgresSQL-viz\postgresql-42.7.5.jar")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
)
    
 
    #deserialize the data from the kafka topic votes_topic
    vote_schema = StructType([
    StructField("voter_id", StringType(), True),
    StructField("candidate_id", StringType(), True),
    StructField("voting_time", TimestampType(), True),
    StructField("voter_name", StringType(), True),
    StructField("party_affiliation", StringType(), True),
    StructField("biography", StringType(), True),
    StructField("campaign_platform", StringType(), True),
    StructField("photo_url", StringType(), True),
    StructField("candidate_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("registration_number", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postcode", StringType(), True)
    ]), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("cell_number", StringType(), True),
    StructField("picture", StringType(), True),
    StructField("registered_age", IntegerType(), True),
    StructField("vote", IntegerType(), True)
])
    
    votes_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "votes_topic") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), vote_schema).alias("data")) \
    .select("data.*")

    def write_to_bq(batch_df, epoch_id):
        (batch_df
            .write.format("bigquery")
            .option("table", "raw_data.raw_vote_events")     # dataset.table
            .option("writeMethod", "direct")                 # Use Storage Write API
            .option("parentProject", "data-stream-pipeline") # 👈 Correct key
            .mode("append")
            .save())


    (votes_df
    .writeStream
    .outputMode("append")
    .foreachBatch(write_to_bq)
    .option("checkpointLocation", r"C:\Users\oluwa\Desktop\Project\End-to-End Data-Pipeline-for-Election-Voting-system-kafka-spark-postgresSQL-viz\checkpoints\checkpoint1")
    .start()
    .awaitTermination())