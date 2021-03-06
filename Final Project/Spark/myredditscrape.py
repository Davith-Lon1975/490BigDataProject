# -*- coding: utf-8 -*-
"""MyRedditScrape.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1cx-R9Gp6dDygXLoAMS4aKfOm_velfmR-

# PySpark SetUp
"""

!apt update
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://apache.osuosl.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
!tar xf spark-3.1.1-bin-hadoop2.7.tgz
!pip install -q findspark

!pip install afinn

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop2.7"

import findspark
import string
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
import string
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, StopWordsRemover, Tokenizer, IDF
from pyspark.ml.classification import  NaiveBayes, LogisticRegression, LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import re
from afinn import Afinn
import pandas



spark = SparkSession.builder.appName("MyRedditScrape").getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

"""# Loading the Data

"""

df = spark.read.option("header", "false").option("quote", "\"").option("escape", "\"").option("inferSchema", "true").csv("/content/drive/MyDrive/490/Data/cleanData.csv").toDF("selftext", "subreddit")

# Take look and make sure everything is ok
df.show()

"""# Labeling (AFINN)"""

# Create out Afinn object
afin = Afinn(language='en')

afin.score("I want to die")

afin.score("I love the world")

type(afin.score("I want to die"))

# This Function will return 1 if the text is negative and 0 if the text is positive.
# This is based on the scoring from the Afinn object
udfNew = F.udf(lambda x: 1 if afin.score(x) < 0 else 0)

data = df.select(F.col('selftext'), udfNew(F.col('selftext')).alias('label'))
data = data.withColumn("label", F.col("label").cast("int"))

data.show()

data.registerTempTable("dataWithLabel")

sqlContext.sql("SELECT label, COUNT(*) as count from dataWithLabel GROUP BY label").show()

"""# Even Out the Data"""

temp1 = sqlContext.sql("SELECT * from dataWithLabel WHERE label = 1 LIMIT 50000")

temp2 = sqlContext.sql("SELECT * from dataWithLabel WHERE label = 0 LIMIT 50000")

data2 = temp1.union(temp2)

data2 = data2.withColumn("label", data2["label"].cast(IntegerType()))
data2 = data2.withColumn("selftext", data2["selftext"].cast(StringType()))

"""# Sample our Data"""

# We take a 10% Sample of our data
data = data2.sample(False, 0.01)

"""# Set Up Elements in Pipeline"""

tokenizer = Tokenizer(inputCol="selftext", outputCol="words")

remover = StopWordsRemover(inputCol="words", outputCol="filtered", caseSensitive=False)

hashingTF = HashingTF(inputCol="filtered", outputCol="rawfeatures", numFeatures= 4096)

idf = IDF(inputCol="rawfeatures", outputCol="features", minDocFreq= 0)

lr = LogisticRegression(regParam=0.01, threshold=0.5)

nb = NaiveBayes()

lsvc = LinearSVC(regParam= 0.01, threshold=0.5)

pipeline1 = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])

pipeline2 = Pipeline(stages=[tokenizer, remover, hashingTF, idf, nb])

pipeline3 = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lsvc])

"""# Test Train Split"""

# splits[0] is my training set, splits[1] is my testing set
splits = data.randomSplit([0.9, 0.1], 1234)

"""#Modeling and Predicting"""

# Logistic Regression Model
model1 = pipeline1.fit(splits[0])

# Naive Bayes Model
model2 = pipeline2.fit(splits[0])

# Linear Support Vector Classification Model
model3 = pipeline3.fit(splits[0])

"""# Logistic Regression Analysis"""

predictions1 = model1.transform(splits[1])

predictions1.show()

# Binary Classification Evaluator


eval1 = BinaryClassificationEvaluator(metricName="areaUnderROC")
print("Area Under the ROC Curve: {}".format(eval1.evaluate(predictions1)))

# Multiclass Classification Evaluator


eval2 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
print("Accuracy: " + str(eval2.evaluate(predictions1)))

eval3 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")

print("Precision: " + str(eval2.evaluate(predictions1)))

"""# Naives Bayes Analysis"""

predictions2 = model2.transform(splits[1])

predictions2.show()

# Binary Classification Evaluator


eval4 = BinaryClassificationEvaluator(metricName="areaUnderROC")
print("Area Under the ROC Curve: {}".format(eval1.evaluate(predictions2)))

# Multiclass Classification Evaluator


eval5 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
print("Accuracy: " + str(eval5.evaluate(predictions2)))

eval6 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")

print("Precision: " + str(eval6.evaluate(predictions2)))

"""# Linear Support Vector Classification Analysis"""

predictions3 = model3.transform(splits[1])

predictions3.show()

# Binary Classification Evaluator


eval7 = BinaryClassificationEvaluator(metricName="areaUnderROC")
print("Area Under the ROC Curve: {}".format(eval7.evaluate(predictions3)))

# Multiclass Classification Evaluator


eval8 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
print("Accuracy: " + str(eval8.evaluate(predictions3)))

eval9 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")

print("Precision: " + str(eval9.evaluate(predictions3)))

"""# Saving the Logistic Regression Model"""

# Save our Logistic Regression Model
model1.save('/content/drive/MyDrive/490/Model')

"""#Resources

https://spark.apache.org/docs/latest/ml-classification-regression.html#logistic-regression

https://spark.apache.org/docs/1.5.1/mllib-naive-bayes.html

https://stackoverflow.com/questions/44779002/pyspark-to-pmml-field-label-does-not-exist-error

https://stackoverflow.com/questions/47707405/spark-logistic-regression-for-binary-classification-apply-new-threshold-for-pre

https://stackoverflow.com/questions/57716806/split-spark-dataframe-in-half-without-overlapping-data

https://hyukjin-spark.readthedocs.io/en/latest/reference/api/pyspark.sql.DataFrame.randomSplit.html

https://stackoverflow.com/questions/40163106/cannot-find-col-function-in-pyspark

https://spark.apache.org/docs/1.6.1/ml-guide.html

https://machinelearningmastery.com/roc-curves-and-precision-recall-curves-for-classification-in-python/


"""