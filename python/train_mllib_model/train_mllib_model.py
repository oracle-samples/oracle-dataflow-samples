#!/usr/bin/env python3

# This example scores customer profiles using a "Recency, Frequency,
# Monetary Value" (RFM) metric.
#
# The data is
#  1. Augmented with continuous scores for each of these metrics.
#  2. These continuous variables are grouped into 4 clusters using a K-Means
#     clustering algorithm.
#  3. Cluster identifiers are used to assign a score along each dimension.
#  4. Scores across each dimension are combined to make an aggregate score.
#
# This code was inspired by https://towardsdatascience.com/data-driven-growth-with-python-part-2-customer-segmentation-5c019d150444
#
# Data used in this example comes from Oracle's "Moviestream" dataset.

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    expr,
    max,
    row_number,
)
from pyspark.sql.window import Window

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

parser = argparse.ArgumentParser()
parser.add_argument("--input", default="moviestream_subset.csv")
parser.add_argument("--output", default="customer_scores.csv")
args = parser.parse_args()

spark = SparkSession.builder.appName("rfm_segmentation").getOrCreate()

# Load our raw transaction data.
raw_df = (
    spark.read.format("csv")
    .load(args.input, header="true")
    .withColumn("day", col("day").cast("date"))
)

# Compute continuous values for recency, frequency and monetary value.
customer_df = (
    raw_df.groupBy("cust_id")
    .agg({"day": "max", "actual_price": "sum", "movie_id": "count"})
    .withColumn("recency", expr('datediff(to_date("2020-12-31"), `max(day)`)'))
    .toDF(*"cust_id monetary frequency most_recent recency_days".split())
)

# MLlib requires variables to be in vectors, even if there is only
# one value. These code blocks take our RFM metrics and place them
# into vectors.
#
# This code does not run immediately, it runs during the ML Pipeline.
recency_assembler = VectorAssembler(
    inputCols=["recency_days"],
    outputCol="recency_feature",
    handleInvalid="skip",
)
frequency_assembler = VectorAssembler(
    inputCols=["frequency"],
    outputCol="frequency_feature",
    handleInvalid="skip",
)
monetary_assembler = VectorAssembler(
    inputCols=["monetary"],
    outputCol="monetary_feature",
    handleInvalid="skip",
)

# Define our k-means models for Recency, Frequency and Monetary Value.
# We will cluster into a fixed 4 groups. As before these will not
# execute immediately but during the Pipeline.
recency_kmeans = KMeans(
    featuresCol="recency_feature", predictionCol="recency_cluster"
).setK(4)
frequency_kmeans = KMeans(
    featuresCol="frequency_feature", predictionCol="frequency_cluster"
).setK(4)
monetary_kmeans = KMeans(
    featuresCol="monetary_feature", predictionCol="monetary_cluster"
).setK(4)

# Define the pipeline of all the steps we need in our ML process.
# First we run all assemblers, then we run all KMeans clustering
# processes.
#
# This Pipeline object behaves like an Estimator insofar as you can
# call .fit() on the Pipeline and it will call .fit() on the
# estimators within the pipeline.
#
# See https://spark.apache.org/docs/latest/ml-pipeline.html for more
# information on MLlib Pipelines.
pipeline = Pipeline(
    stages=[
        recency_assembler,
        frequency_assembler,
        monetary_assembler,
        recency_kmeans,
        frequency_kmeans,
        monetary_kmeans,
    ]
)

# Fitting the pipeline gives us our ML model.
model = pipeline.fit(customer_df)

# Make predictions with our model. In MLlib this will add columns
# to our input DataFrame. We specified the new column names in the
# KMeans models, for example a column named "recency_cluster" will
# be added since we specified that in the "recency_kmeans" cluster.
predictions = model.transform(customer_df)

# Each customer is assigned a cluster ID from 1 to 4 for across
# each dimension. Unfortunately these cluster IDs are assigned
# randomly. We want the cluster representing "best" to be given
# a score of 4 and "worst" to have a score of 1.
#
# To do this we create mapping DataFrames that map cluster IDs
# to the desired score. We create one DataFrame for each of
# Recency, Frequency and Monetary.
recency_ranks = (
    predictions.groupBy("recency_cluster")
    .agg(max("recency_days").alias("max_recency_days"))
    .withColumn(
        "recency_score",
        row_number().over(Window().orderBy(col("max_recency_days").desc())),
    )
)
frequency_ranks = (
    predictions.groupBy("frequency_cluster")
    .agg(max("frequency").alias("max_frequency"))
    .withColumn(
        "frequency_score", row_number().over(Window().orderBy(col("max_frequency")))
    )
)
monetary_ranks = (
    predictions.groupBy("monetary_cluster")
    .agg(max("monetary").alias("max_monetary"))
    .withColumn(
        "monetary_score",
        row_number().over(Window().orderBy(col("max_monetary"))),
    )
)

# Pare the DataFrame down to just the columns we care about.
target_columns = "cust_id monetary frequency most_recent recency_score frequency_score monetary_score".split()

# Compute our scored dataset. This code:
#  1. Maps the cluster scores to meaningful ranks.
#  2. Adds the 3 meaninful ranks into an aggregate score.
rfm = (
    predictions.join(recency_ranks, "recency_cluster")
    .join(frequency_ranks, "frequency_cluster")
    .join(monetary_ranks, "monetary_cluster")
    .select(target_columns)
    .withColumn("score", expr("recency_score + frequency_score + monetary_score"))
)

# This line prevents re-computing the DataFrame when we show the count.
rfm.cache()

# Save the full scored dataset to CSV format.
rfm.write.mode("overwrite").option("header", True).csv(args.output)

# Print out a summary of what happened.
print(
    "Scored {} rows with average score of {}".format(
        rfm.count(), rfm.agg(avg("score")).first()[0]
    )
)
print("Data written to {}".format(args.output))
