### OCI Dataflow - TPCDS Testing
<details>
<summary><font size="2">Check for Public Internet Access</font></summary>

```python
import requests
response = requests.get("https://oracle.com")
assert response.status_code==200, "Internet connection failed"
```
</details>
<details>
<summary><font size="2">Helpful Documentation </font></summary>
<ul><li><a href="https://docs.oracle.com/en-us/iaas/data-flow/using/sql-endpoints.htm">Dataflow SQL Endpoints Documentation</a></li>
<li><a href="https://pypi.org/project/JayDeBeApi/">Python JDBC Bridge</a></li>
</ul>
</details>
<details>
<summary><font size="2">Setting up the Notebook environment using a terminal </font></summary>

```python

$ conda install -c anaconda jaydebeapi

### Install the simba jar files from the SQL-EP connectivity to a known directory 

mkdir -p ~/simba/jars
cd ~/simba
wget https://objectstorage.us-phoenix-1.oraclecloud.com/p/cbpd3lkbUvffH1RXG2CFPmzahEwqrCW9opX_aZRdKA5Jp6q5k6ot1pOV9p959DLv/n/paasdevsss/b/sql-endpoint-artifacts/o/simba-jdbc-driver/SimbaSparkJDBC-2.6.18.2034.zip
 
unzip SimbaSparkJDBC-2.6.18.2034.zip
unzip SimbaSparkJDBC42-2.6.18.2034.zip
mv bcprov-jdk15to18-1.68.jar  SparkJDBC42.jar  jars/

### Download the activation jar
$ cd ~/simba/jars
$ wget https://repo1.maven.org/maven2/javax/activation/activation/1.1.1/activation-1.1.1.jar

```

</details>

<details>
<summary><font size="2">Useful Environment Variables</font></summary>

```python
import os
print(os.environ["NB_SESSION_COMPARTMENT_OCID"])
print(os.environ["PROJECT_OCID"])
print(os.environ["USER_OCID"])
print(os.environ["TENANCY_OCID"])
print(os.environ["NB_REGION"])
```
</details>

<details>
<summary><font size="2">Add any one of the policy statements below to grant DATAFLOW_SQL_ENDPOINT_CONNECT to the notebook</font></summary>

```python
Allow any-user to {DATAFLOW_SQL_ENDPOINT_CONNECT} in tenancy
Allow any-user to {DATAFLOW_SQL_ENDPOINT_CONNECT} in tenancy WHERE ALL {request.principal.type='datasciencenotebooksession'}
Allow any-user to {DATAFLOW_SQL_ENDPOINT_CONNECT} in tenancy WHERE ALL {request.principal.type='datasciencenotebooksession', > request.principal.id='<datasciencenotebooksession OCID>'}
Allow any-user to {DATAFLOW_SQL_ENDPOINT_CONNECT} in tenancy WHERE ALL {request.principal.type='datasciencenotebooksession', request.principal.id='<datasciencenotebooksession OCID>', target.dataflow-sqlendpoint.id='<dataflowsqlendpoint OCID>'}
```

</details>