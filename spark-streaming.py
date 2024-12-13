from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum
import os
from dotenv import load_dotenv

load_dotenv()

PATH_PG_JAR = os.getenv('PATH_PG_JAR')
PATH_CHECKPOINT1 = os.getenv('PATH_CHECKPOINT1')
PATH_CHECKPOINT2 = os.getenv('PATH_CHECKPOINT2')

if __name__ == "__main__":
    
    scala_version = '2.12'  # TODO: Ensure this is correct
    spark_version = '3.5.3'
    packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
        'org.apache.kafka:kafka-clients:3.2.0'
    ]
    
    # Initializing Sparksession   
    spark = (SparkSession.builder
        .appName("RealtimeVotingEngineering")
        .master("local[*]")  # Use local Spark execution with all available cores
        .config("spark.jars.packages", ",".join(packages)) # Spark-Kafka integration
        .config("spark.jars", PATH_PG_JAR)  # PostgreSQL driver
        .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
        .getOrCreate()
    )
            
    # Define schemas for Kafka topics
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
    
    # Read data from Kafka 'votes_topic' and process it
    votes_df = (spark.readStream
        .format('kafka') 
        .option('kafka.bootstrap.servers', 'localhost:9092')
        .option('subscribe', 'votes_topic')
        .option('startingOffsets', 'earliest')
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col('value'), vote_schema).alias('data'))
        .select('data.*')
    )
    
    # # Data preprocessing: typecasting and watermarking
    votes_df = votes_df.withColumn('voting_time', col('voting_time').cast(TimestampType())) \
                .withColumn('vote', col('vote').cast(IntegerType()))
    # Defines an event time, it tracks a point in time before which we assume no more late data is going to arrive
    enriched_votes_df = votes_df.withWatermark('voting_time', '1 minute')
    

    # Aggregate votes per candidate and turnout by location
    votes_per_candidate = enriched_votes_df.groupBy('candidate_id', 'candidate_name', 
                            'party_affiliation', 'photo_url').agg(_sum('vote').alias('total_votes'))

    turnout_by_location = enriched_votes_df.groupBy('address.state').count().alias('total_votes')

    # Write aggregated data to Kafka topics ('aggregated_votes_per_candidate', 'aggregated_turnout_by_location')
    # checkpointLocation is going to prevent reprocessing of the already processed data
    votes_per_candidate_to_kafka = (votes_per_candidate.selectExpr('to_json(struct(*)) AS value')
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggregated_votes_per_candidate")
        .option("checkpointLocation", PATH_CHECKPOINT1)
        .outputMode("update")
        .start()
    )
    
    turnout_by_location_to_kafka = (turnout_by_location.selectExpr('to_json(struct(*)) AS value')
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggregated_turnout_by_location")
        .option("checkpointLocation", PATH_CHECKPOINT2)
        .outputMode("update")
        .start()
    )

    # Await termination for the streaming queries
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()