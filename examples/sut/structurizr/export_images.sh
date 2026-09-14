#!/usr/bin/env bash

# Calculate directory where this script is located.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

if [ ! -e structurizr-preview.war ]
then
  echo "Please download structurizr-preview.war"
  echo "curl -O https://download.structurizr.com/structurizr-preview.war"
  exit 1
fi

java -jar structurizr-preview.war export --workspace workspace.json --format svg --url http://localhost:8080/workspace/1/diagrams --output ../images
