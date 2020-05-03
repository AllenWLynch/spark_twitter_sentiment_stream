

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import types
from pyspark.sql import functions
import os
import re

#training data
DATA_DIR = r'D:\datasets\twitter_sentiment_data'
field_names = ['label','id','date','query','user','text']
field_types = [types.IntegerType(), types.IntegerType(), types.DateType(), types.StringType(), types.StringType(), types.StringType()]

TWEET_DB_SCHEMA = types.StructType([
    types.StructField(field_name, field_type, True) for field_name, field_type in zip(field_names, field_types)
])

#socket params
HOST = 'localhost'
PORT = 9009

#model params
POS_THRESHOLD = 0.66
NEG_THRESHOLD = 0.33
MODEL_NAME = 'LR_with_emoji_pipeline'

EMOJI_PATTERN = (u"(["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "])")

#REMOVE_PATTERN = u")"
REMOVE_PATTERN = (u"|(#\w+)"
        u"|(@[a-zA-Z0-9_]{1,15})"
        u"|https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
        u"|[\\.,\\?<>#@+\\!])")

EMOJI_RE = re.compile(EMOJI_PATTERN, re.UNICODE)