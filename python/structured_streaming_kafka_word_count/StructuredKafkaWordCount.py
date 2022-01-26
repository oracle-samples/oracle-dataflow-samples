#!/usr/bin/env python3

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import concat, col, current_timestamp, lit, window, \
  substring, to_timestamp, explode, split, length

import argparse
import os


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--auth-type', default='PLAIN')
  parser.add_argument('--bootstrap-port', default='9092')
  parser.add_argument('--bootstrap-server')
  parser.add_argument('--checkpoint-location')
  parser.add_argument('--encryption', default='SASL_SSL')
  parser.add_argument('--ocid')
  parser.add_argument('--output-location')
  parser.add_argument('--output-mode', default='file')
  parser.add_argument('--stream-password')
  parser.add_argument('--raw-stream')
  parser.add_argument('--stream-username')
  args = parser.parse_args()

  if args.bootstrap_server is None:
    args.bootstrap_server = os.environ.get('BOOTSTRAP_SERVER')
  if args.raw_stream is None:
    args.raw_stream = os.environ.get('RAW_STREAM')
  if args.stream_username is None:
    args.stream_username = os.environ.get('STREAM_USERNAME')
  if args.stream_password is None:
    args.stream_password = os.environ.get('STREAM_PASSWORD')

  assert args.bootstrap_server is not None, "Kafka bootstrap server (--bootstrap-server) name must be set"
  assert args.checkpoint_location is not None, "Checkpoint location (--checkpoint-location) must be set"
  assert args.output_location is not None, "Output location (--output-location) must be set"
  assert args.raw_stream is not None, "Kafka topic (--raw-stream) name must be set"

  spark = (
    SparkSession.builder
      .appName('StructuredKafkaWordCount')
      .config('failOnDataLoss', 'true')
      .config('spark.sql.streaming.minBatchesToRetain', '10')
      .config('spark.sql.shuffle.partitions', '1')
      .config('spark.sql.streaming.stateStore.maintenanceInterval', '300')
      .getOrCreate()
  )

  # Uncomment following line to enable debug log level.
  # spark.sparkContext.setLogLevel('DEBUG')

  # Configure Kafka connection settings.
  if args.ocid is not None:
    jaas_template = 'com.oracle.bmc.auth.sasl.ResourcePrincipalsLoginModule required intent="streamPoolId:{ocid}";'
    args.auth_type = 'OCI-RSA-SHA256'
  else:
    jaas_template = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";'

  raw_kafka_options = {
    'kafka.sasl.jaas.config': jaas_template.format(
      username=args.stream_username, password=args.stream_password,
      ocid=args.ocid
    ),
    'kafka.sasl.mechanism': args.auth_type,
    'kafka.security.protocol': args.encryption,
    'kafka.bootstrap.servers': '{}:{}'.format(args.bootstrap_server,
                                              args.bootstrap_port),
    'subscribe': args.raw_stream,
    'kafka.max.partition.fetch.bytes': 1024 * 1024,
    'startingOffsets': 'latest'
  }

  # Reading raw Kafka stream.
  raw = spark.readStream.format('kafka').options(**raw_kafka_options).load()

  # Cast raw lines to a string.
  lines = raw.selectExpr('CAST(value AS STRING)')

  # Split value column into timestamp and words columns.
  parsedLines = (
    lines.select(
      to_timestamp(substring('value', 1, 25))
        .alias('timestamp'),
      lines.value.substr(lit(26), length('value') - 25)
        .alias('words'))
  )

  # Split words into array and explode single record into multiple.
  words = (
    parsedLines.select(
      col('timestamp'),
      explode(split('words', ' ')).alias('word')
    )
  )

  # Do time window aggregation
  wordCounts = (
    words
      .withWatermark('timestamp', '1 minutes')
      .groupBy('word', window('timestamp', '1 minute'))
      .count()
      .selectExpr('CAST(window.start AS timestamp) AS START_TIME',
                  'CAST(window.end AS timestamp) AS END_TIME',
                  'word AS WORD',
                  'CAST(count AS long) AS COUNT')
  )

  # Reduce partitions to a single.
  wordCounts = wordCounts.coalesce(1)

  wordCounts.printSchema()

  # Output it to the chosen channel.
  if args.output_mode == 'console':
    print("Writing aggregates to console")
    query = (
      wordCounts.writeStream
        .option('checkpointLocation', args.checkpoint_location)
        .outputMode('complete')
        .format('console')
        .option('truncate', False)
        .start()
    )
  else:
    print("Writing aggregates to Object Storage")
    query = (
      wordCounts.writeStream
        .format('csv')
        .outputMode('append')
        .trigger(processingTime='1 minutes')
        .option('checkpointLocation', args.checkpoint_location)
        .option('path', args.output_location)
        .start()
    )

  query.awaitTermination()


main()
