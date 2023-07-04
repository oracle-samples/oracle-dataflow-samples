#!/bin/bash

# Read command-line arguments
while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
    -a|--all)
      source mysql.sh
      source git.sh
      source docker.sh
      source firewall.sh
      source image.sh
      shift # past argument
      shift # past value
      ;;
    -g|--git)
      source git.sh
      shift # past argument
      ;;
    -d|--docker)
      source docker.sh
      shift # past argument
      ;;
    -s|--mysql)
      source mysql.sh
      shift # past argument
      ;;
    -f|--firewall)
      source firewall.sh
      shift # past argument
      ;;
    -i|--image)
      source image.sh
      shift # past argument
      ;;
    *)    # unknown option
      shift # past argument
      ;;
  esac
done
