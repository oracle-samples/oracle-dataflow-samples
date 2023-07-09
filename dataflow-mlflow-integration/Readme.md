## How To set MLFlow Tracking Server

Change the directory to modules
`cd modules`

In `modules/build.sh` the `build.sh` is a Bash script that allows you to execute various commands related to MySQL, Git, Docker, Firewall, and Image operations.

## Usage

The script accepts several command-line arguments to execute specific tasks. Here are the available options:

- `-a` or `--all`: Executes commands related to MySQL, Git, Docker, Firewall, and Image operations.
- `-g` or `--git`: Executes commands related to Git operations.
- `-d` or `--docker`: Executes commands related to Docker operations.
- `-s` or `--mysql`: Executes commands related to MySQL operations.
- `-f` or `--firewall`: Executes commands related to Firewall operations.
- `-i` or `--image`: Executes commands related to Image operations.

You can provide multiple command-line arguments at once to perform multiple tasks simultaneously.

## Example

To execute all the available commands, you can run the script with the `-a` or `--all` option:
`./build.sh -a`

## MLFlow Tracking Server
`mlflow.sh` script accepts several command-line arguments to set specific variables. Here are the available options:

- `-a` or `--mlflow-artifact-root`: Sets the `MLFLOW_DEFAULT_ARTIFACT_ROOT` variable to the provided value.
- `-s` or `--mlflow-artifacts-destination`: Sets the `MLFLOW_ARTIFACTS_DESTINATION` variable to the provided value.
- `-u` or `--mlflow-backend-store-uri`: Sets the `MLFLOW_BACKEND_STORE_URI` variable to the provided value.
- `-i` or `--docker-image`: Sets the `DOCKER_IMAGE` variable to the provided value.

You need to provide the corresponding values for each option.

## Example

To set the MLflow and Docker variables, you can run the script with the appropriate options and values. For example:

```bash
./mlflow.sh -a /path/to/artifact/root -s /path/to/artifacts/destination -u mysql+mysqlconnector://{username}:{password}@{host}:{db_port}/{db_name} -i mydockerimage:latest
