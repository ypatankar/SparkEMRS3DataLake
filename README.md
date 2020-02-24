# Data Lake with AWS S3, EMR and Spark

This project extracts json data from S3, processes data on EMR using Apache Spark, transforms it into parquet format and stores it back on S3. 

## Getting Started

Cloning the repository will get you a copy of the project up and running on your local machine for development and testing purposes. 

- `git@github.com:ypatankar/SparkEMRS3DataLake.git`
- `https://github.com/ypatankar/SparkEMRS3DataLake.git`

### Prerequisites

* AWS account
* Amazon EMR cluster with Spark and YARN
* S3 bucket to write parquet files
* pyspark library

### Contents

* `etl.py` : Processes all data files from S3, loads data in EMR, processes by Spark, tranforms to 5 parquet format files and stores back on S3
* `dl.cfg` : Contains AWS account configuration

### Data Storage and Partition

Parquet file name | Partition
--- | --- 
songplay | `year`, `month` 
song | `year`, `artist_id` 
artist | no partition 
user | no partition 
time | `year`, `month`  

### Deployment Steps
1. SSH into EMR cluster and execute `etl.py` using /usr/bin/spark-submit --master <url> --deploy-mode "cluster" etl.py
