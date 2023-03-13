# -*- coding: utf-8 -*-
"""
Created on Tue Feb 28 14:26:11 2023

@author: Vishnu Prasanth
"""

# Initialize a source credential which does not have access to list bucket:

import os
import json
from google.cloud import bigquery
import pandas as pd    
from google.oauth2 import service_account
from google.auth import impersonated_credentials
from google.cloud import secretmanager


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'service_account_json_key'

def access_secret_info(project_id, secret_id, secret_version_id):
    
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{secret_version_id}"
    # name = "projects/100000/secrets/secret-sa-etl/versions/1"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})
    
    # print("Response Generated - " , response)
    secret = response.payload.data.decode('UTF-8')
    
    secretkey = json.loads(secret)
    
    return secretkey



def impersonate_sa(target_principal):
    
    target_scopes = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/bigquery.insertdata',
        'https://www.googleapis.com/auth/devstorage.full_control']
    
    project_id = "eu-dxone-353411"
    secret_id = "secret-hsad-eu-dxone-sa-secret-reader"
    secret_version_id = 1 #Airflow variable can be used here

    secretkey = access_secret_info(project_id, secret_id, secret_version_id)
    
    source_credentials = ( 
        service_account.Credentials.from_service_account_info(secretkey,
            scopes=target_scopes))

# Now use the source credentials to acquire credentials to impersonate another service account:

    target_credentials = impersonated_credentials.Credentials(
      source_credentials=source_credentials,
      target_principal= target_principal,
      target_scopes = target_scopes,
      lifetime=600)
    
    return target_credentials


gcp_sa_etl = "gcp-sa-etl@google_project.iam.gserviceaccount.com"

# target_credentials = impersonate_sa(gcp_sa_etl)

bq_client = bigquery.Client(credentials = impersonate_sa(gcp_sa_etl))

q1 = """select session_user();"""

df = bq_client.query(q1).to_dataframe()


from google.cloud import storage

gcs_client = storage.Client(credentials=impersonate_sa(gcp_sa_etl))

buckets = gcs_client.list_buckets(project = "project_id")

for bucket in buckets:
  print(bucket.name)
