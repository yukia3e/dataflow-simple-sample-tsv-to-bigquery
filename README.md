# Dataflow Simple Sample (TSV in GCS to BigQUery)
## Sample Summary
Simple Dataflow sample to store TSV files in GCS to BigQuery

- File name pattern "test_file_{target_data_date (YYYYMMDD) }_{output_date (YYYYMMDDHHMMSS)}" (If there are multiple files with the same target date, the latest date's file is automatically targeted)
- The first line in the file is a header line. We specify the string included at the beginning of the header line as a way to skip it in the beam.
- Assume that the table is created in BigQuery in advance to match the schema. (When partitioning with tdate, the program sets the date of the file suffix to tdate, allowing for constant control of the amount of data when searching.
- Assumed to run on a GCP console

## Execution procedure
### 1. Pre-execution preparation
#### 1-1. Pre-qualification of GCP projects
- The Dataflow API must be enabled for the project

#### 1-2. Clone your repository with Cloud Shell
Clone this repository (in this case, save it in a directory called "dataflow")

#### 1-3. Installing pip and the Cloud Dataflow SDK
```
cd dataflow/
sudo pip3 install -U pip
sudo pip3 install --upgrade virtualenv
virtualenv -p python3.7 env
source env/bin/activate
sh install_packages.sh
```

### 2. Execution
#### 2-1. Direct (local execution)
```
cd dataflow/
source env/bin/activate

gcloud config set project [PROJECT_ID]

【importer.py の実行コマンド】
python importer_direct.py --project [PROJECT_ID] --storagebucket [STORAGE_BUCKET_NAME] --workbucket [WORK_BUCKET_NAME] --dataset [BIGQUERY_DATASET_NAME] --tdate [TARGET_DATE]
```

PROJECT_ID：   
GCP project ID to be executed.

STORAGE_BUCKET_NAME:  
The name of the Cloud Storage bucket in which the files are stored.

WORK_BUCKET_NAME:  
Cloud Storage bucket name for the DataFlow work directory

BIGQUERY_DATASET_NAME:
The name of the BigQuery dataset.

TARGET_DATE:
Date to be processed (in YYYYMMDD format): 204200401.


#### 2-2. Dataflowの場合
```
cd dataflow/
source env/bin/activate

gcloud config set project [hopstar-dev/hopstar-prod]

【importer.py の実行コマンド】
python importer.py --project [PROJECT_ID] --storagebucket [STORAGE_BUCKET_NAME] --workbucket [WORK_BUCKET_NAME] --dataset [BIGQUERY_DATASET_NAME] --tdate [TARGET_DATE]
```

PROJECT_ID：   
GCP project ID to be executed.

STORAGE_BUCKET_NAME:  
The name of the Cloud Storage bucket in which the files are stored.

WORK_BUCKET_NAME: 
Cloud Storage bucket name for the DataFlow work directory

BIGQUERY_DATASET_NAME:
The name of the BigQuery dataset.

TARGET_DATE:
Date to be processed (in YYYYMMDD format): 204200401.