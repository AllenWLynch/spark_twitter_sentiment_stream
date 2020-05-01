

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


pos_model = ml.PipelineModel.load(config.MODEL_NAME)
neg_model = ml.PipelineModel.load(config.MODEL_NAME)

pos_model.stages[-1].setThreshold(config.POS_THRESHOLD)
neg_model.stages[-1].setThreshold(config.NEG_THRESHOLD)


#%%
SESSION_ID = str(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

#%% Change this the most!
TWEETS_OUTPUT = './output_test/output'
BATCH_DIAGNOSTIC_OUTPUT = './output_test/batches'


def save_function(df, batch_id):
    df.persist()
    df.write.mode("append").format("json").save(TWEETS_OUTPUT)
    agg_df = df.agg(F.min("created_at_unix").alias("created_at_unix"), F.count("text").alias("count"))
    agg_df.persist()
    agg_df = agg_df.withColumn("timestamp", F.current_timestamp())
    agg_df = agg_df.withColumn("latency", (F.unix_timestamp("timestamp") - F.col("created_at_unix").alias("latency")))
    agg_df = agg_df.withColumn("batch_id", F.lit(batch_id))
    agg_df.write.mode("append").format("json").save(BATCH_DIAGNOSTIC_OUTPUT)
    agg_df.unpersist()
    df.unpersist()

#%%
streamed_tweets = spark.readStream\
        .format("socket")\
        .option("host", config.HOST)\
        .option("port", config.PORT)\
        .load()

#%%
#receive -> convert to columns -> convert dates -> save

#1. convert to columns
streamed_tweets = streamed_tweets.withColumn('json', F.from_json("value", STREAM_SCHEMA)).select("json.*")

#2. apply model
streamed_tweets = pos_model.transform(streamed_tweets)\
    .select("created_at","text","hashtags","place_name","lon","lat","features", F.col("prediction").alias("positive_label"))

streamed_tweets = neg_model.stages[-1].transform(streamed_tweets)\
    .select("created_at","text","hashtags","place_name","lon","lat",(F.col("prediction") + F.col("positive_label")).alias("label"))

#4. reformat dates and calc latency
streamed_tweets = streamed_tweets.withColumn("created_at_unix", F.unix_timestamp("created_at", format = "EEE MMM dd HH:mm:ss Z yyyy")) #unixtime
streamed_tweets = streamed_tweets.withColumn("created_at_timestamp", F.from_unixtime("created_at_unix"))

streamed_tweets = streamed_tweets.withColumn("session_id", F.lit(SESSION_ID))

'''query = streamed_tweets \
    .writeStream \
    .foreachBatch(save_function)\
    .start()'''

#query = streamed_tweets.writeStream.outputMode('append').format('console').start()

query = streamed_tweets.writeStream.outputMode("append").format("json").option("path", "./output").trigger(processingTime='10 seconds').start()

query.awaitTermination()

#%%