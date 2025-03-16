#!/bin/bash
set -e

# Ensure JAR files are available to Spark from the shared volume
if [ -d "/opt/spark_files/jars" ]; then
  echo "Copying JAR files from shared volume to Spark..."
  # Copy JAR files from shared volume to Spark jars directory
  cp -f /opt/spark_files/jars/*.jar /opt/bitnami/spark/jars/ 2>/dev/null || true
  
  # If running as root, set proper permissions
  if [ "$(id -u)" = "0" ]; then
    chmod -R 777 /opt/spark_files
  fi
else
  echo "Warning: JAR files directory not found in shared volume"
fi

# Execute the original entry point
exec /opt/bitnami/scripts/spark/entrypoint.sh "$@" 