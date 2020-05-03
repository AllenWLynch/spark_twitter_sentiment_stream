

#%%
import findspark
findspark.init()

import pyspark

spark = pyspark.sql.SparkSession.builder.appName("Tweet D-stream")\
    .config("spark.executor.memory","2g")\
    .config("spark.driver.memory","1g")\
    .getOrCreate()

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import ml
import os
import datetime
import config
# %%

STREAM_SCHEMA = T.StructType().add("created_at",T.StringType()).add("text",T.StringType()).add("hashtags",T.ArrayType(T.StringType()))\
        .add("place_name", T.StringType()).add("lon", T.FloatType()).add("lat",T.FloatType())


MODEL = ml.PipelineModel.load(config.MODEL_NAME)
PREDICTION_COL = 'prediction'

COLLECTION_TIME = '1 minute'

#%%
SESSION_ID = str(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

#%% Change this the most!
TWEETS_OUTPUT = './output_test/output'
BATCH_DIAGNOSTIC_OUTPUT = './output_test/batches'


streamed_tweets = spark.readStream\
        .format("socket")\
        .option("host", config.HOST)\
        .option("port", config.PORT)\
        .load()

#1. convert to columns
streamed_tweets = streamed_tweets.withColumn('json', F.from_json("value", STREAM_SCHEMA)).select("json.*")

#2. apply model
streamed_tweets = MODEL.transform(streamed_tweets)\
    .select("created_at","hashtags","place_name","lon","lat",F.col(PREDICTION_COL).alias("sentiment"))

streamed_tweets = streamed_tweets.withColumn("created_at", 
    F.unix_timestamp("created_at", format = "EEE MMM dd HH:mm:ss Z yyyy").cast(T.TimestampType()))

    
streamed_tweets = streamed_tweets.withColumn("hashtags", F.when(F.size("hashtags") > 0, F.concat_ws("hashtags")).otherwise("none"))

#aggregate
streamed_tweets = streamed_tweets.withWatermark("created_at", COLLECTION_TIME)\
    .groupBy(
        F.window("created_at", COLLECTION_TIME, COLLECTION_TIME),
        "hashtags",
        "place_name",
    ).agg(
        F.sum("sentiment").alias("num_positive"),
        F.count("*").alias('total'),
        F.first("lat").alias("lat"),
        F.first("lon").alias("lon"),
    )

streamed_tweets = streamed_tweets.withColumn('num_negative', F.col('total') - F.col('num_positive'))

query = streamed_tweets.writeStream.outputMode('append').format('console').trigger(processingTime=COLLECTION_TIME).start()

query.awaitTermination()

#%%