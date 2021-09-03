#!/usr/bin/env python3
# Copyright © 2021, Oracle and/or its affiliates. 
# The Universal Permissive License (UPL), Version 1.0 as shown at https://oss.oracle.com/licenses/upl.

import os
import sys
import traceback
import pandas as pd

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import string
import re
from pyspark.sql.functions import udf
from pyspark.ml.feature import *
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors as MLLibVectors
from pyspark.mllib.classification import SVMWithSGD
from pyspark.ml.evaluation import MulticlassClassificationEvaluator



def main():

    # Set up Spark.
    spark_session = get_dataflow_spark_session()
    sql_context = SQLContext(spark_session)

    db_name = sys.argv[1]

    queryMetaStore(spark_session, db_name)
    


def get_dataflow_spark_session(app_name="DataFlow", file_location=None, profile_name=None, spark_config={}):
    """
    Get a Spark session in a way that supports running locally or in Data Flow.
    """
    if in_dataflow():
        spark_builder = SparkSession.builder.appName(app_name)
    else:
        # Import OCI.
        try:
            import oci
        except:
            raise Exception(
                "You need to install the OCI python library to test locally"
            )

        # Use defaults for anything unset.
        if file_location is None:
            file_location = oci.config.DEFAULT_LOCATION
        if profile_name is None:
            profile_name = oci.config.DEFAULT_PROFILE

        # Load the config file.
        try:
            oci_config = oci.config.from_file(
                file_location=file_location, profile_name=profile_name
            )
        except Exception as e:
            print("You need to set up your OCI config properly to run locally")
            raise e
        conf = SparkConf()
        conf.set("fs.oci.client.auth.tenantId", oci_config["tenancy"])
        conf.set("fs.oci.client.auth.userId", oci_config["user"])
        conf.set("fs.oci.client.auth.fingerprint", oci_config["fingerprint"])
        conf.set("fs.oci.client.auth.pemfilepath", oci_config["key_file"])
        conf.set(
            "fs.oci.client.hostname",
            "https://objectstorage.{0}.oraclecloud.com".format(
                oci_config["region"]),
        )
        spark_builder = SparkSession.builder.appName(
            app_name).config(conf=conf)

    # Add in extra configuration.
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # Create the Spark session.
    session = spark_builder.enableHiveSupport().getOrCreate()
    return session


def in_dataflow():
    """
    Determine if we are running in OCI Data Flow by checking the environment.
    """
    if os.environ.get("HOME") == "/home/dataflow":
        return True
    return False


def queryMetaStore(spark_session, db_name):

    spark_session.sql("USE " + db_name)

    review = spark_session.table(db_name + ".yelp_review")

    # Using SparkSQL, basic data exploratory data analysis could be done. The majority of the reviews focuses on the positive sides -- intuitively, people tend to leave reviews when they feel great about their experience.
    # Contradictory, the number of extremely negative reviews (1 star) also overwhelm the number of neutral reviews (2 or 3 stars).

    review.groupBy('stars').agg(
        count('review_id').alias('count')).sort('stars').show()

    # buildmodel(spark_session,review)         


def buildmodel(spark_session, df):

    punct_remover = udf(lambda x: remove_punct(x))

    rating_convert = udf(lambda x: convert_rating(x))
    # select 1.5 mn rows of reviews text and corresponding star rating with punc removed and ratings converted
    resultDF = df.select('review_id', punct_remover(
        'text'), rating_convert('stars')).limit(1500000)
    # user defined functions change column names so we rename the columns back to its original names
    resultDF = resultDF.withColumnRenamed('<lambda>(text)', 'text')
    resultDF = resultDF.withColumnRenamed('<lambda>(stars)', 'stars')

    # tokenizer and stop word remover

    tok = Tokenizer(inputCol="text", outputCol="words")
    # stop word remover
    stopwordrm = StopWordsRemover(inputCol='words', outputCol='words_nsw')
    # Build the pipeline
    pipeline = Pipeline(stages=[tok, stopwordrm])
    # Fit the pipeline
    review_tokenized = pipeline.fit(resultDF).transform(resultDF).cache()

    # count vectorizer and tfidf
    cv = CountVectorizer(inputCol='words_nsw', outputCol='tf')
    cvModel = cv.fit(review_tokenized)
    count_vectorized = cvModel.transform(review_tokenized)
    idf = IDF(inputCol="words_nsw", outputCol="tf")
    tfidfModel = idf.fit(count_vectorized)
    tfidf_df = tfidfModel.transform(count_vectorized)

    # split into training and testing set
    splits = tfidf_df.select(
        ['tfidf', 'label']).randomSplit([0.8, 0.2], seed=100)
    train = splits[0].cache()
    test = splits[1].cache()

    numIterations = 50
    regParam = 0.3
    svm = SVMWithSGD.train(train, numIterations, regParam=regParam)
    test_lb = test.rdd.map(lambda row: LabeledPoint(
        row[1], MLLibVectors.fromML(row[0])))
    scoreAndLabels_test = test_lb.map(lambda x: (
        float(svm.predict(x.features)), x.label))
    score_label_test = spark_session.createDataFrame(
        scoreAndLabels_test, ["prediction", "label"])

    f1_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1")
    svm_f1 = f1_eval.evaluate(score_label_test)
    print("F1 score: %.4f" % svm_f1)


# clean the reviews text to remove any punctuation or numbers using the following function.


def remove_punct(text):
    regex = re.compile('[' + re.escape(string.punctuation) + '0-9\\r\\t\\n]')
    nopunct = regex.sub(" ", text)
    return nopunct


def convert_rating(rating):
    if rating >= 4:
        return 1
    else:
        return 0

# replace the word with selected ngram




if __name__ == "__main__":
    main()
