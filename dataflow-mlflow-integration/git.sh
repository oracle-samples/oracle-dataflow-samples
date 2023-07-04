#!/bin/bash

# Function to check if a package is installed
package_exists() {
  if rpm -q "$1" >/dev/null 2>&1; then
    return 0  # Package exists
  else
    return 1  # Package does not exist
  fi
}

# Install dependencies
if ! package_exists git; then
  sudo yum install -y git
fi

# Function to check if a directory exists
directory_exists() {
  if [ -d "$1" ]; then
    return 0  # Directory exists
  else
    return 1  # Directory does not exist
  fi
}

# Clone the repository if it is not already cloned
if ! directory_exists oci-mlflow; then
  git clone https://github.com/oracle/oci-mlflow.git
fi

