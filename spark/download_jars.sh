#!/bin/bash

# Create jars directory if it doesn't exist
mkdir -p jars

echo "Downloading Hadoop AWS JAR files..."

# Function to download a JAR file with retry logic
download_jar() {
    local jar_name=$1
    local jar_url=$2
    local max_attempts=3
    local attempt=1
    
    echo "Downloading $jar_name..."
    
    while [ $attempt -le $max_attempts ]; do
        if wget -q -O jars/$jar_name $jar_url; then
            echo "Successfully downloaded $jar_name"
            return 0
        else
            echo "Attempt $attempt failed. Retrying..."
            attempt=$((attempt+1))
            sleep 2
        fi
    done
    
    echo "Failed to download $jar_name after $max_attempts attempts."
    return 1
}

# Download Hadoop AWS jars
download_jar "hadoop-aws-3.3.4.jar" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
download_jar "aws-java-sdk-bundle-1.12.262.jar" "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# Verify downloads
if [ ! -f "jars/hadoop-aws-3.3.4.jar" ] || [ ! -f "jars/aws-java-sdk-bundle-1.12.262.jar" ]; then
    echo "ERROR: Failed to download one or more required JAR files."
    echo "Please check your internet connection and try again."
    exit 1
fi

# Ensure the files are readable
chmod 644 jars/*.jar

echo "All required JAR files have been downloaded successfully." 