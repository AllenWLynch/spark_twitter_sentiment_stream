
import findspark
findspark.init()

import pyspark
import config

spark = pyspark.sql.SparkSession.builder.appName("Tweet D-stream").getOrCreate()

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import ml
import os
import datetime
import argparse


STREAM_SCHEMA = T.StructType().add("created_at",T.StringType()).add("text",T.StringType()).add("hashtags",T.ArrayType(T.StringType()))\
        .add("place_name", T.StringType()).add("lon", T.FloatType()).add("lat",T.FloatType())


PREDICTION_COL = 'prediction'
MODEL = ml.PipelineModel.load("LR_with_emoji_pipeline")

COLLECTION_TIME = '1 minute'

parser = argparse.ArgumentParser(description='Spark sentiment analysis stream.')
parser.add_argument('outputdir', type = str, help = 'Output dir for stream')
parser.add_argument('--format', '-f', type = str, help = "Output format for stream (parquet, json, etc.)")
parser.add_argument("--port", "-p", type = int, default=9009, help ="TCP socket port number. Default is 9009.")
parser.add_argument("--ip", "-i", type = str, default = "localhost", help = "TCP socket destination IP. Default is localhost.")

if __name__ == "__main__":

    args = parser.parse_args()
	assert(args.port > 0), 'port argument must be greater than 0.'
    assert(os.path.isdir(args.outputdir)), 'outputdir does not exist'

    data_dir = os.path.join(args.outputdir, 'data')
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    checkpointdir = os.path.join(args.outputdir, 'checkpoints')
    if not os.path.exists(checkpointdir):
        os.mkdir(checkpointdir)
    
    streamed_tweets = spark.readStream\
        .format("socket")\
        .option("host", args.ip)\
        .option("port", args.port)\
        .load()

    #1. convert to columns
    streamed_tweets = streamed_tweets.withColumn('json', F.from_json("value", STREAM_SCHEMA)).select("json.*")

    #2. apply model
    streamed_tweets = MODEL.transform(streamed_tweets)\
        .select("created_at","hashtags","place_name","lon","lat",F.col(PREDICTION_COL).alias("sentiment"))

    streamed_tweets = streamed_tweets.withColumn("created_at", 
        F.unix_timestamp("created_at", format = "EEE MMM dd HH:mm:ss Z yyyy").cast(T.TimestampType()))

    streamed_tweets = streamed_tweets.fillna("none", subset = ['hashtags','place_name'])

    streamed_tweets = streamed_tweets.withColumn("hashtags", F.concat_ws('|', 'hashtags'))

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

    streamed_tweets = streamed_tweets.withColumn('start_window', F.col('window.start'))
    streamed_tweets = streamed_tweets.withColumn('end_window', F.col('window.end'))

    streamed_tweets = streamed_tweets.drop('window')

    stream = streamed_tweets.writeStream.outputMode('append')\
        .trigger(processingTime=COLLECTION_TIME)\
        .format('parquet')\
        .option("path", data_dir)\
        .option("checkpointLocation", checkpointdir)

    stream.start()
    stream.awaitTermination()