import functions_framework

import os
import requests

def start_databricks_job(event, context):
    """
    Trigger Databricks job with Google Cloud Storage file URI path
    """
    # Extract the file name and path from the event payload
    file_name = event['name']
    file_path = f"gs://{event['bucket']}/{file_name}"
    
    # Set the Databricks API endpoint, API token, and job ID
    databricks_domain = os.environ.get('DATABRICKS_DOMAIN')
    databricks_api_key = os.environ.get('DATABRICKS_API_KEY')
    databricks_job_id = os.environ.get('DATABRICKS_JOB_ID')
    run_databricks_url = f"https://{databricks_domain}/api/2.0/jobs/run-now"
    
    # Define the payload to pass to the Databricks REST API
    payload = {
        'job_id': databricks_job_id,
        'notebook_params': {
            'file_path': file_path
        }
    }
    
    # Set the headers to include the API token
    headers = {
        'Authorization': f'Bearer {databricks_api_key}',
        'Content-Type': 'application/json'
    }
    
    # Send the POST request to trigger the Databricks job with the file path parameter
    response = requests.post(run_databricks_url, json=payload, headers=headers)
    
    if response.status_code == 200:
        print(f"Triggered Databricks job {databricks_job_id} with file path {file_path}")
    else:
        raise Exception(f"Failed to trigger Databricks job: {response.content}")