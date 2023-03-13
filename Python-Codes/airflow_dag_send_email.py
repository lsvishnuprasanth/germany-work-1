# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 16:52:06 2023

@author: Vishnu Prasanth
"""


from __future__ import print_function


############## Uncoment below while deploring dag ###########
from airflow import models
from airflow.decorators import task
# from airflow.operators import bash_operator
# from airflow.operators import python_operator
from airflow.models import Variable
from airflow.utils.email import send_email


#------------------------------------------------------------
# from google.cloud import storage
from datetime import datetime,  timedelta
# import pandas as pd
# from google.cloud import bigquery
# import yaml
# from util.functions import bq_db_connect,query_bq_source,load_bq_target,log,read_file_from_gcs
# from util.sftp_functions import load_to_bq_temp, load_to_bq_stg, archive_file,sftp_data_load_main
from util.sftp_functions import sftp_file_check


yesterday = datetime.now() - timedelta(1)

file_date = yesterday.strftime('%Y%m%d')

#today = datetime.today().date()
#today_date = today.strftime('%Y-%m-%d')

#Input Values List
country_list = ['FR']
table_list = ['Table_name']
bucket_name = 'bucket_name'




default_dag_args = {
    'retries' : 1,
    'retry_delay': timedelta(minutes=5),
    'start_date':  datetime(yesterday.year,yesterday.month,yesterday.day),
    #"email": ["sample@gmail.com"],
    #"email_on_failure": True, 
    #'email_on_retry': False,
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'cdp_sftp_send_files_status_email',
        schedule_interval= Variable.get("sftp_schedule_for_airflow_jobs", default_var=None),
        default_args=default_dag_args) as dag:

#############################################################  
# Main Function for Multiple Country 

    @task
    # Data processing     
    def sftp_file_check_main():
        
        status_list_yes = []
        status_list_no = []
        y_cnt = 0
        n_cnt = 0
        
        for i in country_list:
            for j in table_list:
                status = sftp_file_check(i,j)
                if status.split('-')[-1] == 'Yes':
                    y_cnt+=1
                    status_list_yes.append((str(y_cnt)+ '. ' +status).replace('-Yes',''))
                    
                else:
                    n_cnt+=1
                    status_list_no.append((str(n_cnt)+ '. ' +status).replace('-No',''))
                    
                    
        y ="<br>".join(status_list_yes)
        n ="<br>".join(status_list_no)
        
        #print(f"Files Available \n{y}\n ")
        #print(f"Files Not Available \n{n}\n")
        
        #print(f"<h2><U> Files Available </U></h2>\n{y}\n ")
        #print(f"<h2><U> Files Not Available </U></h2> \n{n}\n")
        if len(status_list_no) > 0:
            email_subject = f"""SFTP File Status -  {file_date} - ({n_cnt} files not available) """
        else:
            email_subject = "SFTP File Status - " + file_date
        
        if len(y) == 0:
            email_body = f"""
            <h2 style=\"color: #ff5753;\"><U> Files Not Available  </U></h2> <br>
            <font style=\"color: #ff5753;\"> {n} </font> <br> """
        elif len(n) == 0:
            email_body = f"""
            <h2 style=\"color: #3E902C;\"><U> Files Available  </U></h2><br> 
            <font style=\"color: #3E902C;\"> {y} </font><br> """
        else:
            email_body = f"""
            <h2 style=\"color: #3E902C;\"><U> Files Available  </U></h2><br> 
            <font style=\"color: #3E902C;\"> {y} </font> <br> 
            <h2 style=\"color: #ff5753;\"><U> Files Not Available  </U></h2> <br>
            <font style=\"color: #ff5753;\"> {n} </font> <br> """
        
        send_email('vishnu.ls@hsadgermany.com', email_subject, email_body)

                  
    
    sftp_file_check_main()
    
    #result= sftp_data_load.expand(country=country_list)
       
    #sum_it(result)




#############################################################  

       #  def main():
       #      #Main Code Begins
       #      sftp_data_load_main(country, table_name)
              
       # # data_load()
       #  def all_good():
       #      print("All Good...")

   # =============================================================================
        # completed = python_operator.PythonOperator(
        #    task_id='completed',
        #    python_callable=all_good)
          
        # main = python_operator.PythonOperator(
        #      task_id='sftp_data_load',
        #      python_callable=main)
       
   # =============================================================================
   #       Define the order in which the tasks complete by using the >> and << operators.
   # =============================================================================
       # main >> completed    

   ###########Version v1 ######################################################     
      
