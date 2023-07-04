#!/bin/bash

# Function to check if a package is installed
package_exists() {
  if rpm -q "$1" >/dev/null 2>&1; then
    return 0  # Package exists
  else
    return 1  # Package does not exist
  fi
}

# Install dependencies only if they are missing
if ! package_exists yum-utils; then
  sudo yum install -y yum-utils
fi

if ! package_exists docker-ce; then
  sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
  sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

# Start Docker service-Check if Docker is already running
if ! pgrep -x "dockerd" >/dev/null; then
  echo "Docker is not running. Starting Docker..."
  sudo dockerd >/dev/null 2>&1 &
  echo "Docker started."
else
  echo "Docker is already running. No changes needed."
fi
