# Oracle Cloud Infrastructure Data Flow Samples

This repository provides examples demonstrating how to use Oracle Cloud Infrastructure Data Flow, a service that lets you run any Apache Spark Application  at any scale with no infrastructure to deploy or manage.

## What is Oracle Cloud Infrastructure Data Flow

Data Flow is a cloud-based serverless platform with a rich user interface. It allows Spark developers and data scientists to create, edit, and run Spark jobs at any scale without the need for clusters, an operations team, or highly specialized Spark knowledge. Being serverless means there is no infrastructure for you to deploy or manage. It is entirely driven by REST APIs, giving you easy integration with applications or workflows. You can:

* Connect to Apache Spark data sources.

* Create reusable Apache Spark applications.

* Launch Apache Spark jobs in seconds.

* Manage all Apache Spark applications from a single platform.

* Process data in the Cloud or on-premises in your data center.

* Create Big Data building blocks that you can easily assemble into advanced Big Data applications.

## Before you Begin

* You must have Set Up Your Tenancy and be able to Access Data Flow

  * Setup Tenancy : Before Data Flow can run, you must grant permissions that allow effective log capture and run management. See the Set Up Administration[Set Up Administration](https://docs.oracle.com/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin) section of Data Flow Service Guide, and follow the instructions given there.  
  * Access Data Flow : Refer to this section on how to [Access Data Flow](https://docs.oracle.com/en-us/iaas/data-flow/data-flow-tutorial/getting-started/dfs_tut_get_started.htm#access_ui)

## Sample Examples

| Example            | Description | Python |
|-------------------|:-----------:|:------:|
| CSV to Parquet    |This application shows how to use PySpark to convert CSV data store in OCI Object Store to Apache Parquet format which is then written back to Object Store.              |[sample](./python/csv_to_parquet)|
| Load to ADW       |This application shows how to read a file from OCI Object Store, perform some transformation and write the results to an Autonomous Data Warehouse instance.              |[sample](./python/loadadw)|

For step-by-step instructions, see the README files included with
each sample.

## Running the Samples

These samples show how to use the OCI Data Flow service and are meant
to be deployed to and run from Oracle Cloud. You can optionally test
these applications locally before you deploy them.  When they are ready, you can deploy them to Data Flow without any need to reconfigure them, make code changes, or apply deployment profiles.To test these applications locally, Apache Spark needs to be installed. Refer to section on how to set the Prerequisites before you deploy the application locally [Setup locally](https://docs.oracle.com/en-us/iaas/data-flow/data-flow-tutorial/develop-apps-locally/front.htm).

## Install Spark

To install Spark, visit [spark.apache.org](https://spark.apache.org/docs/latest/api/python/getting_started/index.html)
and pick the installation path that best suits your environment.

## Documentation

You can find the online documentation for OCI Data Flow at [docs.oracle.com](https://docs.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm).

## Security

See [Security](./SECURITY.md)

## Contributing

See [CONTRIBUTING](./CONTRIBUTING.md)

## License

See [LICENSE](./LICENSE.txt)
