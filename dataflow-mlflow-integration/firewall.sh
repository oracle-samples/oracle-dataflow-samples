#!/bin/bash

# Check if the port is already allowed
sudo firewall-cmd --zone=public --query-port=5000/tcp
port_status=$?

# Add the port if it is not already allowed
if [[ $port_status -ne 0 ]]; then
  echo "Port 5000 is not allowed. Adding the rule..."
  sudo firewall-cmd --zone=public --permanent --add-port=5000/tcp
  sudo firewall-cmd --reload
  echo "Firewall rules updated."
else
  echo "Port 5000 is already allowed. No changes needed."
fi
