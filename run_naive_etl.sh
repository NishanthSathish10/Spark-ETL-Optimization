#!/bin/bash
# Wrapper script to run naive_etl.py with correct Java version

# Set JAVA_HOME to Java 17 (required for Spark 4.0+, LTS version with best compatibility)
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

# Update PATH to use Java 17 first
export PATH="$JAVA_HOME/bin:$PATH"

# Verify Java version
echo "Using Java from: $JAVA_HOME"
java -version

# Activate virtual environment if not already activated
if [ -z "$VIRTUAL_ENV" ]; then
    source .venv/bin/activate
fi

# Run the naive ETL script
python naive_etl.py

