#!/bin/bash
# Wrapper script to run optimized_etl.py with correct Java version

# Set JAVA_HOME to Java 17 (required for Spark 4.0+, LTS version with best compatibility)
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

# Update PATH to use Java 17 first
export PATH="$JAVA_HOME/bin:$PATH"

# Set Java options for Spark to use more memory (for 24GB RAM machine)
export JAVA_OPTS="-Xmx16g -Xms16g -XX:+UseG1GC"

# Verify Java version
echo "Using Java from: $JAVA_HOME"
java -version

# Activate virtual environment if not already activated
if [ -z "$VIRTUAL_ENV" ]; then
    source .venv/bin/activate
fi

# Run the optimized ETL script
python optimized_etl.py

