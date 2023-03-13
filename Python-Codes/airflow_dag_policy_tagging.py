# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 13:47:56 2023

@author: Vishnu Prasanth
"""

from __future__ import print_function

from airflow import models
from airflow.operators import python_operator
from airflow.hooks.base_hook import BaseHook
#--------------------------------------------------------------
# import os
from datetime import datetime, timedelta
from google.cloud import bigquery
from util.policy_tagging_functions import get_pii_datasets, get_pii_tables, get_policy_tags,policy_tag_check
from util.functions import config_extract,bq_db_connect_ver2

# #Define Variable
PKL_FILE_NAME="pii_column_details.pkl"
yesterday = datetime.now() - timedelta(1)
connection3= BaseHook.get_connection('cdp_etl_bq_service_acc') # cdp_pii_etl_bq_service_acc
CDP_PII_TARGET_SA_ID=connection3.login

default_dag_args = {
    'retries' : 1,
    'start_date':  datetime(yesterday.year,yesterday.month,yesterday.day),
}

with models.DAG(
        "cdp_assign_policy_tag_to_pii_tables",
        schedule_interval="40 19 * * *",
        tags=["util"],
        default_args=default_dag_args) as dag:

#############################################################     
        
        def assign_policy_tag():
            print("Process Started ...")
                       
            bq_client=bq_db_connect_ver2(CDP_PII_TARGET_SA_ID)
            # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =r'D:\Projects\CDP\CDP - Users\hsad-eu-dxone-sa-etl\eu-dxone-353411-9a5637929045.json'
            
            # Getting PII datasets
            pii_datasets =  get_pii_datasets(bq_client)        
            print("PII Datasets Identified : ", pii_datasets)
            
            # Getting PII Tables
            pii_tables = get_pii_tables(pii_datasets,bq_client)
            print("Total Number of PII Tables Identified : ", len(pii_tables))                     
            
            # reading pii_columns_details file as dataframe
            PKL_FILE_NAME="pii_column_details.pkl"
            pii_df=config_extract(source_table_name=None, pkl_file_name=PKL_FILE_NAME)
            table_list=pii_df["Table"].unique()
            
            pii_table_not_tagged = policy_tag_check(pii_tables, pii_df,bq_client)
            
            print(pii_table_not_tagged)
            
            # bq_client = bigquery.Client()
            
            #Get tables not in CSV file
            table_not_in_csv = []
            
            #Reading all policy tags in the project as dictionary
            policy_tag_dict = get_policy_tags()
            
            # pii_tables1 = ['ds_prod_CDP_dw_fr.LGCM_V_L0LGCO_ACC_CUSTOMER_WITHDRAWAL_M_X2003243_8902']
            
            # columns = ['dataset_id', 'table_name', 'total_cols', 'pii_cols', 'non_pii_cols']
            # pii_log_df_list = list()
            #pii_table_not_tagged
            
            print("Assigning Policy Tags ...")
            
            for table in pii_table_not_tagged:                
                # if table.split('.')[1] == check_table:
                table_to_check = table[:table.find("_X2003243")]
                
                if table_to_check.split('.')[1] in table_list:
                    tablename = bq_client.get_table(table)
                    new_schema = []
                    # get table schema
                    table_schema = tablename.schema

                    pii_cols = 0
                    non_pii_cols = 0
                    
                    for cols in table_schema:
                        if cols.policy_tags != None:    
                            # print("1", cols.name)
                            new_schema.append(bigquery.SchemaField(name = cols.name,  
                                              field_type = cols.field_type,
                                              description = cols.description,
                                              policy_tags = cols.policy_tags
                                              )
                                              )
                        else:
                            #if cols.name in pii_df['Column_Name'].unique():                             
                            filter_df=pii_df[(pii_df['Column_Name']==cols.name) & (pii_df['Table']==table_to_check.split('.')[1])] #tablename.table_id
                            if len(filter_df) > 0:
                                # print("2", cols.name)
                                column_desc = filter_df['Policy_Tag_Name'].item()
                                policy_tag_column = policy_tag_dict[column_desc]
                                # print(policy_tag_column)
                                new_schema.append(bigquery.SchemaField(name=cols.name,  
                                                  field_type=cols.field_type,
                                                  description= column_desc,
                                                  policy_tags=bigquery.PolicyTagList([policy_tag_column])
                                                  )
                                                  )
                                pii_cols+=1
                            else:
                            # print("3", cols.name)
                            # print("Non PII Columns", cols.name)
                                new_schema.append(bigquery.SchemaField(name=cols.name,  
                                              field_type=cols.field_type                                
                                              )
                                              )
                                non_pii_cols+=1

                                                       
                    # print(tablename.dataset_id,tablename.table_id,total_cols, pii_cols, non_pii_cols)
                    
                    # Updating the Table with Policy Tags and Column Description
                    tablename.schema = new_schema
                    updated_table = bq_client.update_table(tablename, ['schema'])  
                    # pii_log_df_list.append(pd.DataFrame([[tablename.dataset_id,tablename.table_id,total_cols, pii_cols, non_pii_cols]], columns=columns))
                    # new_row = f""" {'table_name'} """
                else:
                    table_not_in_csv.append(table)
                    
            # pii_log_df = pd.concat(pii_log_df_list)
            
            
            print("PII Tables not in csv file : ", table_not_in_csv )
            print("Process Completed...")
            
        def all_good(): 
            print("All Good....")

# =============================================================================
             
        assign_policy_tag = python_operator.PythonOperator(
              task_id='assign_policy_tag',
              python_callable=assign_policy_tag)
        
        completed = python_operator.PythonOperator(
            task_id='completed',
            python_callable=all_good)
    
# =============================================================================
#       Define the order in which the tasks complete by using the >> and <<  operators.
# =============================================================================
        assign_policy_tag >> completed    
