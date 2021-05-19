Copyright © 2021, Oracle and/or its affiliates. 
The Universal Permissive License (UPL), Version 1.0

Overview:
These samples show how to use OCI Data Flow to run Apache Spark
applications to process data in Oracle Cloud Infrastructure. To run
these samples you will need an Oracle Cloud account and access to the
Data Flow service within that account.

The applications included are:
1) csv_to_parquet.py : This application shows how to use PySpark to
   convert CSV data stored in OCI Object Store to Apache Parquet format,
   which is then written back to Object Store.
2) loadadw.py : This application shows how to read a file from OCI
   Object Store, perform some transformation and write the results to
   an Autonomous Data Warehouse instance.

For step-by-step instructions, see the README.txt files included with
each sample.

Running the Samples:
These samples show how to use the OCI Data Flow service and are meant
to be deployed to and run from Oracle Cloud. You can optionally test
these applications locally before you deploy them. To test these
applications locally, Apache Spark needs to be installed.

To install Spark, visit https://spark.apache.org/docs/latest/api/python/getting_started/index.html
and pick the installation path that best suits your environment.
