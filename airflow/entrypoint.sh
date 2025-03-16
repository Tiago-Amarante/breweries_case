#!/bin/bash
set -e

# Ensure the spark_files directories exist and have correct permissions
if [ ! -d "/opt/spark_files/jars" ]; then
  mkdir -p /opt/spark_files/jars
fi

if [ ! -d "/opt/spark_files/data" ]; then
  mkdir -p /opt/spark_files/data
fi

# Try to set permissions if running as root
if [ "$(id -u)" = "0" ]; then
  chmod -R 777 /opt/spark_files
fi

# Run the original entrypoint
exec /usr/bin/dumb-init -- /entrypoint "$@" 