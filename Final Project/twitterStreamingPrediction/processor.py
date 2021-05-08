from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.ml import PipelineModel
from pyspark.sql import Row
import sys
import re

# Initializing spark session
sc = SparkContext(appName="PySparkShell")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

# Loading the pretrained model on Reddit data
model = PipelineModel.load('C:\\Users\\Akkip\\Documents\\Model')

# Function to preprocess data and perform prediction on  received tweets
def get_prediction(tweet_text):
    try:
        # Filter out all the mentions and links
        tweet_text = tweet_text.map(lambda l: re.sub(r"(?:\@|https?\://)\S+", "", l))
        # Filter out all the other special characters
        tweet_text = tweet_text.map(lambda l: re.sub(r"[^a-zA-Z0-9]+", ' ', l).lower())
        # Filter the tweets to include those with length > 0
        tweet_text = tweet_text.filter(lambda x: len(x) > 0)
        # create a dataframe with column name 'selftext' and each row will contain the tweetText
        rowRdd = tweet_text.map(lambda w: Row(selftext=w))
        # create a spark dataframe
        wordsDataFrame = spark.createDataFrame(rowRdd)     
        # transform the data using the pipeline and get the predicted sentiment
        model.transform(wordsDataFrame).select('selftext','prediction').show()
    except : 
        e = sys.exc_info()
        print(e)


# initialize the streaming context 
ssc = StreamingContext(sc, batchDuration= 3)
# Create a DStream that will connect to hostname:port, like localhost:9009
# localhost:9009 is the stream we are doing
tweetsline = ssc.socketTextStream("localhost",9009)
# split the tweet text by apliting at line break to get the list of tweets
tweets = tweetsline.flatMap(lambda line : line.split('\n'))
# send the rdd for prediction from trained model
tweets.foreachRDD(get_prediction)
# Start the computation
ssc.start()             
# Wait for the computation to terminate
ssc.awaitTermination()
