#!/bin/bash

# Set default values
DEFAULT_REGION="phx"
DEFAULT_TENANCY="paasdevssspre"
DEFAULT_TAG="latest"

# Read inputs
# Read region input
read -p "Enter region: " region

if [[ -n "$region" ]]; then
    # Read tenancy input
    read -p "Enter tenancy [my-tenancy]: " tenancy
    tenancy=${tenancy:-$DEFAULT_TENANCY}

    # Read tag input
    read -p "Enter tag [my-tag]: " tag
    tag=${tag:-$DEFAULT_TAG}
else
    # Use default values for tenancy and tag
    region=$DEFAULT_REGION
    tenancy=$DEFAULT_TENANCY
    tag=$DEFAULT_TAG
fi

cd oci-mlflow
sudo docker build -t "$region.ocir.io/$tenancy/oci-mlflow:$tag" --network host -f container-image/Dockerfile .
cd ..
