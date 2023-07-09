#!/bin/bash


# Function to check if a package is installed
package_exists() {
  if rpm -q "$1" >/dev/null 2>&1; then
    return 0  # Package exists
  else
    return 1  # Package does not exist
  fi
}

if ! package_exists mysql-shell; then
  sudo yum install -y mysql-shell
fi

# Create the database
echo "Enter the username of the Database:"
read username


echo "Enter the Hostname:"
read hostname
echo "Enter the Password:"
read -s password
echo "Enter the Database name:"
read dbname

# Create Database
mysqlsh "$username@$hostname" --password="$password" --sql -e "CREATE DATABASE $dbname;"
