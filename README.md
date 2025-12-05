SPARK-ETL-OPTIMIZATION

This project analyzes the NYC Taxi & Limousine Commission (TLC) trip record dataset to demonstrate performance optimization techniques in Apache Spark.

The goal is to take a "naive" ETL pipeline, establish a performance baseline, and systematically optimize it to reduce runtime, tail latency, and resource costs.

Getting Started

1. Prerequisites

Before you begin, you will need to install uv, a fast Python package installer and virtual environment manager.

Follow the official installation instructions:

```curl -LsSf [https://astral.sh/uv/install.sh](https://astral.sh/uv/install.sh) | sh```


After installation, you may need to restart your terminal or source your shell's profile (e.g., source ~/.zshrc).

2. Environment Setup

Once uv is installed, you can set up the project environment:

```

# 1. Clone this repository
# (Assuming you've already done this)
# git clone ...
# cd spark-optimization-project

# 2. Install dependencies
uv sync

# 3. Activate the new environment
source .venv/bin/activate

```

Your environment is now fully set up and ready to run the scripts.

3. Download Data

Run the data downloader script to fetch the NYC Taxi data for 2011-2024. This will take some time and download approx. 30gigabytes of data into a data/ directory.

```
python data_downloader.py
```

4. Run the Baseline ETL Job

Execute the "naive" ETL script using spark-submit. This job will run several transformations and establish our initial performance baseline.

```
spark-submit \
  --driver-memory 12g \
  naive_etl.py > naive_etl_output.txt

```

5. Run the Optimized ETL Job

Execute the "optimized" ETL script using spark-submit.

```
spark-submit \
  --driver-memory 12g \
  optimized_etl.py > optimized_etl_output.txt

```

While the job is running, you can monitor its progress and view detailed system metrics at the Spark UI, typically available at http://localhost:4040.