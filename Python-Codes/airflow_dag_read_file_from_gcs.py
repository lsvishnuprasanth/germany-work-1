# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 17:40:19 2022

@author: Vishnu Prasanth
"""

from __future__ import print_function


############## Uncoment below while deploring dag ###########
from airflow import models
from airflow.decorators import task
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.models import Variable
#------------------------------------------------------------
from google.cloud import storage
from datetime import datetime, timezone, timedelta
import pandas as pd
from google.cloud import bigquery
import yaml
from util.functions import bq_db_connect,query_bq_source,load_bq_target,log,read_file_from_gcs
from util.sftp_functions import load_to_bq_temp, load_to_bq_stg, archive_file,sftp_data_load_main



table_name = 'TB_MP_USR_SVC_DETAIL'

country_list = ['FR']
yesterday = datetime.now() - timedelta(1)
today = datetime.today()


default_dag_args = {
    'retries' : 1,
    'retry_delay': timedelta(minutes=5),
    'start_date':  datetime(yesterday.year,yesterday.month,yesterday.day),
    #"email": ["vishnu.ls@hsadgermany.com", "v.pathak@hsadgermany.com"],
    #"email_on_failure": True, 
    #'email_on_retry': False,
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'cdp_sftp_EMPL_'+ table_name,
        schedule_interval= Variable.get("sftp_schedule_for_airflow_jobs", default_var=None),
        tags=["SFTP","PII"],
        default_args=default_dag_args) as dag:

#############################################################  
# Main Function for Multiple Country 

    @task
    # Data processing     
    def sftp_data_load(country):
           
        days = Variable.get("sftp_schedule_days", default_var=None)
        
        i = datetime.today() - timedelta(int(days))
        
        today = datetime.today()
        
        while i <= today:
            
            file_status = sftp_data_load_main(country, table_name, i )
            
            print(file_status)
            
            i = i + timedelta(1)
        
        #email_subject_success = country + " : " + table_name + ' : Success !'
        email_subject_failure = country + " : " + table_name + ' : Failed .. !'

    result= sftp_data_load.expand(country=country_list)

