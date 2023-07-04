#!/bin/bash


# Function to check if a package is installed
package_exists() {
  if rpm -q "$1" >/dev/null 2>&1; then
    return 0  # Package exists
  else
    return 1  # Package does not exist
  fi
}

# Function to check if a string is empty or blank
is_blank() {
  if [[ -z "$1" ]]; then
    return 0  # String is blank
  else
    return 1  # String is not blank
  fi
}

if ! package_exists mysql-shell; then
  sudo yum install -y mysql-shell
fi

# Create the database
echo "Enter the username of the Database:"
read username

# If username is blank, use default credentials
if is_blank "$username"; then
  username="admin"
  password="Mlflow@2023"
  hostname="mysqldb.sub01292340360.datasciencevcn.oraclevcn.com"
  dbname="mlflow"
else
  echo "Enter the Hostname:"
  read hostname
  echo "Enter the Password:"
  read -s password
  echo "Enter the Database name:"
  read dbname
fi

# Create Database
mysqlsh "$username@$hostname" --password="$password" --sql -e "CREATE DATABASE $dbname;"
