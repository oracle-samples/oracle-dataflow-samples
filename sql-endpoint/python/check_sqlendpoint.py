#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Usage: python check_sqlendpoint.py <sql_endpoint_ocid>"

import oci
import requests
import json
import time
import sys

# Replace with your region and OCID
#endpoint_ocid = "ocid1.dataflowsqlendpoint.oc1.phx.anyhqljtkiytkuaawaywh34klpefxdfkjnl6jrflvbyg22vrtruxlasj5t5a"  # <-- Replace this
poll_interval = 60     # seconds between checks
timeout = 6000          # max time to wait in seconds

# Load config and create signer for raw requests
config = oci.config.from_file("~/.oci/config", profile_name='SQLEP') # or specify profile: ("~/.oci/config", "DEFAULT")
region = config["region"]
# OCI base URL for sqlendpoint operations
base_url = f"https://dataflow.{region}.oci.oraclecloud.com/20200129/sqlEndpoints"

token_file = config['security_token_file']
token = None
with open(token_file, 'r') as f:
     token = f.read()

private_key = oci.signer.load_private_key_from_file(config['key_file'])
signer = oci.auth.signers.SecurityTokenSigner(token, private_key)
client = oci.identity.IdentityClient({'region': region}, signer=signer)


def get_endpoint_status(ocid):
    url = f"{base_url}/{ocid}"
    response = requests.get(url, auth=signer)
    if response.status_code == 200:
        lifecycle_state = response.json()["lifecycleState"]
        print(f"[INFO] SQL Endpoint status: {lifecycle_state}")
        return lifecycle_state
    else:
        print(f"[ERROR] Failed to get endpoint status: {response.status_code} {response.text}")
        return None

def start_sql_endpoint(ocid):
    url = f"{base_url}/{ocid}/actions/start"
    response = requests.post(url, auth=signer)
    if response.status_code in [200, 202]:
        print("[INFO] Start request sent successfully.")
    else:
        print(f"[ERROR] Failed to start endpoint: {response.status_code} {response.text}")

def get_sql_endpoint_status(ocid):
    url = f"{base_url}/{ocid}"
    response = requests.get(url, auth=signer)
    if response.status_code == 200:
        status = response.json().get("lifecycleState")
        print(f"[INFO] Current SQL Endpoint status: {status}")
        return status
    else:
        print(f"[ERROR] Failed to get status: {response.status_code} {response.text}")
        return None

def wait_until_available(ocid, timeout, interval):
    print("[INFO] Waiting for SQL Endpoint to become AVAILABLE...")
    start_time = time.time()

    while True:
        status = get_sql_endpoint_status(ocid)

        if status == "AVAILABLE":
            print("[SUCCESS] SQL Endpoint is now AVAILABLE.")
            return True
        elif status in ["STOPPING", "STOPPED", "STARTING", "UPDATING"]:
            elapsed = int(time.time() - start_time)
            if elapsed > timeout:
                print("[ERROR] Timeout reached while waiting for endpoint to become AVAILABLE.")
                return False
            print(f"[INFO] Status is {status}. Retrying in {interval}s...")
            time.sleep(interval)
        else:
            print(f"[ERROR] Unexpected status: {status}")
            return False


def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <sql_endpoint_ocid>")
        sys.exit(1)

    endpoint_ocid = sys.argv[1]

    status = get_endpoint_status(endpoint_ocid)

    if status and status != "AVAILABLE":
        print("[INFO] Starting the SQL endpoint...")
        start_sql_endpoint(endpoint_ocid)
        wait_until_available(endpoint_ocid, timeout, poll_interval)
    elif status == "AVAILABLE":
        print("[INFO] SQL endpoint is already active.")
    else:
        print("[ERROR] Could not determine endpoint status.")

if __name__ == "__main__":
    main()
