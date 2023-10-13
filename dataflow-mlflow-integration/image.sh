#!/bin/bash

# Read inputs
# Read region input
read -p "Enter region: " region


# Read tenancy input
read -p "Enter tenancy [my-tenancy]: " tenancy

# Read tag input
read -p "Enter tag [my-tag]: " tag

cd oci-mlflow
sudo docker build -t "$region.ocir.io/$tenancy/oci-mlflow:$tag" --network host -f container-image/Dockerfile .
cd ..
