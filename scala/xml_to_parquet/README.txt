XML to Parquet example.

-> Get the Data
Download data from https://registry.opendata.aws/irs990/

-> Load Data to OCI.
Transfer the XML files to OCI in your bucket of choice.

-> Build the Spark application.
Run "sbt assembly"

-> Upload the Spark application to OCI Object Store.
Upload IRS990-assembly-0.6.0-SNAPSHOT.jar to your bucket of choice.

-> Create the Data Flow application.
The Spark application expects two arguments: (1) is the source bucket where
you put the XML files (2) is the destination path where the Parquet table is
written.
