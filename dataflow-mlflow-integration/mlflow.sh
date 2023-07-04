#!/bin/bash

# Read command-line arguments
while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
    -a|--mlflow-artifact-root)
      MLFLOW_DEFAULT_ARTIFACT_ROOT="$2"
      shift # past argument
      shift # past value
      ;;
    -s|--mlflow-artifacts-destination)
      MLFLOW_ARTIFACTS_DESTINATION="$2"
      shift # past argument
      shift # past value
      ;;
    -u|--mlflow-backend-store-uri)
      MLFLOW_BACKEND_STORE_URI="$2"
      shift # past argument
      shift # past value
      ;;
    -i|--docker-image)
      DOCKER_IMAGE="$2"
      shift # past argument
      shift # past value
      ;;
    *)    # unknown option
      shift # past argument
      ;;
  esac
done

# Set default values if not provided
MLFLOW_DEFAULT_ARTIFACT_ROOT=${MLFLOW_DEFAULT_ARTIFACT_ROOT:-oci://nilay-mlflow@paasdevssspre/}
MLFLOW_ARTIFACTS_DESTINATION=${MLFLOW_ARTIFACTS_DESTINATION:-oci://nilay-mlflow@paasdevssspre/}
MLFLOW_BACKEND_STORE_URI=${MLFLOW_BACKEND_STORE_URI:-mysql+mysqlconnector://admin:Mlflow%402023@mysqldb.sub01292340360.datasciencevcn.oraclevcn.com:3306/mlflow}
DOCKER_IMAGE=${DOCKER_IMAGE:-phx.ocir.io/paasdevssspre/oci-mlflow:latest}

# Run the docker command
sudo docker run --rm \
  --name oci-mlflow \
  --network host \
  -e MLFLOW_HOST=0.0.0.0 \
  -e MLFLOW_GUNICORN_OPTS='--log-level debug' \
  -e MLFLOW_PORT=5000 \
  -e MLFLOW_DEFAULT_ARTIFACT_ROOT="$MLFLOW_DEFAULT_ARTIFACT_ROOT" \
  -e MLFLOW_ARTIFACTS_DESTINATION="$MLFLOW_ARTIFACTS_DESTINATION" \
  -e BACKEND_PROVIDER=mysql \
  -e MLFLOW_BACKEND_STORE_URI="$MLFLOW_BACKEND_STORE_URI" \
  -e MLFLOW_SERVE_ARTIFACTS=1 \
  -e OCIFS_IAM_TYPE=instance_principal \
  "$DOCKER_IMAGE"
