# Python Predictions: Leveraging GCP for Data Orchestration and Analysis

In this project, I utilized Google Cloud Platform (GCP) to create an orchestrated pipeline and store data in BigQuery for more centralized and efficient data analysis.

## Workflow Overview

1. I began by uploading the JSON files to Cloud Storage.
2. Subsequently, a native table was created for data analysis.
3. Afterwards, I leveraged GCP Databricks for data processing. The DBFS was utilized for the storage of all JSON files, which were then processed in accordance to the specifications of the test assessment.

For more detailed information on the loading aspect of the process into BigQuery, please refer to the [Load BQ file](https://github.com/lhenriquear/python-predictions/blob/main/Load%20BQ.py).

## Automating the Workflow

In order to automate the workflow, a Cloud Function was created that triggers whenever a new file is uploaded to the bucket in Cloud Storage. This function, in turn, invokes a Databricks job (details can be found in the [job.json file](https://github.com/lhenriquear/python-predictions/blob/main/job.json). This job uses as source code the file [Pipeline.py](https://github.com/lhenriquear/python-predictions/blob/main/Pipeline.py)

For a better understanding of the flow of operations, please consult the architecture diagram in the [arch.png file](https://github.com/lhenriquear/python-predictions/blob/main/arch.png).

## Data Analysis and Viewing

For data analysis, I recommend using a BigQuery query as it preserves the JSON structure of the fields. 

To access the data table in BigQuery, please use a GCP Account to query the table `chrome-parity-398807.airquality.aqiTable`.

Here's an example of a query:

```sql
SELECT * FROM `chrome-parity-398807.airquality.aqiTable`
```

Besides BigQuery, the data can also be viewed in the CSV file present in this repository.

> **Note
   All deployed services will be decommissioned on Friday, 22/09/2023.

