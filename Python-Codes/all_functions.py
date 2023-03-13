# -*- coding: utf-8 -*-
"""
Created on Thu Oct 13 15:13:07 2022

@author: p.vikas
"""
from __future__ import print_function
from airflow.hooks.base_hook import BaseHook
from datetime import datetime,timedelta,timezone
import pandas as pd
import pickle
from google.cloud import bigquery
import os
import yaml
import logging
from google.cloud import storage
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.auth import impersonated_credentials
import requests
import io
import json

# =============================================================================
#  All DAGs will process data for listed country below 
#  Please add the conountry code here to add new country data in data pipeline
# =============================================================================

country_list = ["FR"] #"UK","IT"
DAG_BUCKET="europe-west3-composer-airfl-9c9a4990-bucket"

# =============================================================================
# Function to extract configarations
# =============================================================================
def config_extract(source_table_name=None, pkl_file_name="config_df.pkl"):
    
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    bucket_name=DAG_BUCKET
    blob_name=f"dags/util/{pkl_file_name}" 
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    with blob.open("rb") as f:
        config_df = pickle.load(f)

    
    if source_table_name== None:
        df_fin = config_df
        return df_fin
    
    else:    
        df_fin = config_df[(config_df.source_table_name==source_table_name)]
        if len(df_fin) != 1:
            print("Please make sure the dag table name exists in config file")
            raise Exception("Given table name not found in config, Please check configuration file!")
        return df_fin.values[0]

 
# =============================================================================
# Function to connect Source system 
# =============================================================================
def access_secret_info(project_id, secret_id, secret_version_id):   
    client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{secret_version_id}"
    # name = "projects/506722209115/secrets/secret-hsad-eu-dxone-sa-etl/versions/1"
    # Access the secret version.
    # print(3)
    # response = client.access_secret_version(request={"name": name})
    response = client.access_secret_version(name)
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
    # print(2)
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

def bq_db_connect(key_filepath):
     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =key_filepath  #"C:\\Users\hatch\Downloads\eu-cdp-poc-1162a2b31638.json"
     # Construct a BigQuery client object.
     conn_obj = bigquery.Client()
     print("Database connection: Successful")
     return conn_obj
 
def bq_db_connect_ver2(service_account_name):
    
     target_credentials = impersonate_sa(service_account_name)
     print("Connection database using account:",service_account_name)
     conn_obj = bigquery.Client(credentials=target_credentials)
     print("Database connection: Successful")
     return conn_obj
 
# =============================================================================
# #Function to read data from BQ database 
# =============================================================================

def query_bq_source(full_source_name,connection_obj,where_clouse=" WHERE 1=1",is_partition=False):

    sql_str=f"""
            SELECT *,current_timestamp() AS cdp_load_ts,"" AS cdp_anon_customer_id FROM `{full_source_name}`  {where_clouse}"""
    
    if  'L0LGCO' in full_source_name  or 'GSFS' in full_source_name :
        is_partition=False
    # TODO:: Handdle this with checking partion in source table    
        
    if is_partition:
        #Check partition min, max column 
        partition_sql=f"""SELECT min(P_PTT)  min_date , max(P_PTT) max_date FROM `{full_source_name}`  {where_clouse}"""
        partition_df = connection_obj.query(partition_sql).to_dataframe() 
        min_date=str(partition_df.iloc[0].min_date)
        max_date=str(partition_df.iloc[0].max_date)
        print(f"""Partition_min_date::{min_date}, Partition_max_date::{max_date}""")
        partition_filter=f""" and P_PTT between '{min_date}' and '{max_date}' """
        #reconfigure sql
        sql_str=f"""
         SELECT *,current_timestamp() AS cdp_load_ts,"" AS cdp_anon_customer_id FROM `{full_source_name}` {where_clouse} {partition_filter}"""       
    
    print(sql_str)
    # Excute the SQL Job and convert to dataframe
    source_df = connection_obj.query(sql_str).to_dataframe()  
    print("Total No.of records extracted::",len(source_df))
    return source_df
 
    
# =============================================================================
#  #Function to load BigQuery data 
# =============================================================================

def load_bq_target(connection_obj,source_df,target_table_id,job_config):
     
     job = connection_obj.load_table_from_dataframe(
     source_df, target_table_id, job_config=job_config
     )  # Make an API request.

     print("Target job configure and create:: successful")

     job.result()  # Wait for the job to complete.
     job.output_rows

     print("Data loaded in target table")
     table = connection_obj.get_table(target_table_id)  # Make an API request.
     # Number of records loaded in target table 
     snr=len(source_df)
     print("Total No.of records loaded::", snr)

     print(
     "Total {} rows and {} columns to {} in the table".format(
         table.num_rows, len(table.schema), target_table_id
         )
     )
 
    
# =============================================================================
# Function to process high volume data using OFFSET
# =============================================================================
 
def bulk_chunk_data_load_bq(khq_source_sa_id,target_sa_id,target_table_id,job_config,full_source_name,where_clouse="",order_by_column_name=""):
    
    source_conn=bq_db_connect_ver2(khq_source_sa_id)
    
    #Ger record count
    count_str="select count(*) as cnt from "+full_source_name+" "+where_clouse
    df_count= source_conn.query(count_str).to_dataframe()
    total_row_count=df_count.iloc[0].cnt
    print("row count:",total_row_count)
    
    limit=100000
        
    offset=0
    
    source_sql_str=f"""
                  SELECT *,current_timestamp() AS cdp_load_ts," " AS cdp_anon_customer_id FROM `{full_source_name}` {where_clouse} 
                 ORDER BY  {order_by_column_name} ASC LIMIT {str(limit)} OFFSET {str(offset)}""" 
    
    print (source_sql_str)
    #Creating data chunk
    snr=0
    while total_row_count>offset:
        
        print("=================================================================")
        print("Offset :",offset)
        #Extracting source data 
        source_sql_str="""
        SELECT *,current_timestamp() AS cdp_load_ts,"" AS cdp_anon_customer_id FROM `"""+full_source_name+"` "+where_clouse \
            +" ORDER BY  "+order_by_column_name+" ASC LIMIT "+str(limit)+" OFFSET "+ str(offset) 
        
        source_connection_obj=bq_db_connect_ver2(khq_source_sa_id)
        # Excute the SQL Job and convert to dataframe
        source_df = source_connection_obj.query(source_sql_str).to_dataframe()  
        print("Total No.of records extracted from source::",len(source_df))
        
        if len(source_df)>0:
            
            target_connection_obj=bq_db_connect_ver2(target_sa_id)
            
            
            job = target_connection_obj.load_table_from_dataframe(
            source_df, target_table_id, job_config=job_config
            )  # Make an API request.
    
            job.result()  # Wait for the job to complete.
    
            print("Data loaded in target table")
            # table = target_connection_obj.get_table(target_table_id)  # Make an API request.

        else:
            print(" 0 Record from source")
        
        snr=len(source_df)
        print("Loading Sucessfull!!!. Total No.of records loaded::", snr)
        
        # OFFSET reset  
        offset=offset+limit      
    print("===========================================================")    
    print("Congratulations !!.. Data loading completed with no error..")
    return snr

# =============================================================================
# Function to load data using paggination
# =============================================================================
def bulk_chunk_data_using_pagination(khq_source_sa_id,target_sa_id,target_table_id,job_config,full_source_name,where_clouse=""):
    
    source_query="""
    SELECT *,current_timestamp() AS cdp_load_ts," " AS cdp_anon_customer_id FROM `"""+full_source_name+"` "+where_clouse
  
    source_conn=bq_db_connect_ver2(khq_source_sa_id)
    #Ger record count
    count_str="select count(*) as cnt from `"+full_source_name+"` "+where_clouse
    df_count= source_conn.query(count_str).to_dataframe()
    total_row_count=df_count.iloc[0].cnt
    print("Source row count:",total_row_count)
    
    client = bigquery.Client()
    job = client.query(source_query)
    result = job.result(page_size=100000)
    cnt=0

    target_connection_obj=bq_db_connect_ver2(target_sa_id)
    
    for result_df in result.to_dataframe_iterable():
        # df will have at most 20 rows
        if len(result_df)>0:
            
            job = target_connection_obj.load_table_from_dataframe(
            result_df, target_table_id, job_config=job_config
            )  # Make an API request.

            job.result()  # Wait for the job to complete.
        cnt=cnt+1
        print(" batch loaded ..",cnt) 
        print("Number of record loaded =", len(result_df))
        
    table = target_connection_obj.get_table(target_table_id)  # Make an API request.
    print("Congratulations !!.. Data loading completed with no error..")
    print(
                "Now total {} rows and {} columns to {} in the target table".format(
                    table.num_rows, len(table.schema), target_table_id
                    )
                )
    return table.num_rows

# =============================================================================
#   LOGGING FUNCTION
# =============================================================================
 
def log(source_name="",etl_process_name="",country_name="",source_table_name="",
        target_table_name="",source_row_count=0,target_row_count=0,load_start_dt=datetime.now(),
        load_end_dt=datetime.now(),created_at=datetime.now(),created_by="Airflow ETL", 
        loading_status="",error_message="",source_system_name="", data_load_method=""):

    # !Getting connection info

    connection2= BaseHook.get_connection('cdp_etl_bq_service_acc')
    CDP_TARGET_SA_ID= connection2.login
    

    log_table_id='eu-dxone-353411.ds_logs.etl_log'
    conn_obj=bq_db_connect_ver2(CDP_TARGET_SA_ID)
    
    df_log=pd.DataFrame([[source_name,etl_process_name,country_name,source_table_name,target_table_name,
                          source_row_count,target_row_count,load_start_dt,load_end_dt,created_at,created_by,
                          loading_status,error_message,source_system_name,data_load_method]], 
                          columns=['source_name','etl_process_name','country_name' ,'source_table_name', 
                                   'target_table_name','source_row_count','target_row_count','load_start_dt','load_end_dt', 
                                   'created_at','created_by','loading_status','error_message','source_system_name','data_load_method'])
    
    schema=[]
    job_config = bigquery.LoadJobConfig(
            schema=schema,
            )
    
    job = conn_obj.load_table_from_dataframe(
    df_log, log_table_id, job_config=job_config
    )  # Make an API request.
    
    job.result()    
    print("Logging done in table :",log_table_id)
       
 
# =============================================================================
# Function to read file from GCS bucket 
# =============================================================================

def read_file_from_gcs(file_name ="config.yaml" ):
    
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    if file_name =="config.yaml": 
        bucket_name=DAG_BUCKET
        blob_name="dags/config.yaml" 
        
    elif file_name =="sql_statements.yaml": 
        bucket_name=DAG_BUCKET
        blob_name="dags/util/sql_statements.yaml" 
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open("r") as f:
        config = yaml.safe_load(f)
    return config

# =============================================================================
# Function to Getting Watermark 
# =============================================================================

def setup_watermark(source_system_name="",target_sa_id="",full_target_name="",where_clouse="",load_from_start=True,incremental_date_column=""):
    print("Watermark process started ..")
    target_conn=bq_db_connect_ver2(target_sa_id)
    
    tracking_date_column_name=incremental_date_column
    
    sql=f""" SELECT MAX({tracking_date_column_name}) AS watermark_date FROM  {full_target_name}  """ 
    print(sql)
        
    watermark_df = target_conn.query(sql).to_dataframe()
     
    if (len(watermark_df)==0 or pd.isna(watermark_df.iloc[0].watermark_date)): 
        print("First time load")
        if load_from_start:
            watermark_date="1900-01-01"
        else :
            watermark_date=(datetime.today()- timedelta(days=913)).strftime('%Y-%m-%d')
    else:
        watermark_date=watermark_df.iloc[0].watermark_date
        watermark_date=watermark_date -  timedelta(days=1)
        watermark_date=str(watermark_date)[0:10]
    
    print("Watremark Date:",watermark_date)
    where_clouse=where_clouse+" AND "+tracking_date_column_name +" >= '"+watermark_date+"'"
    print("New Where clouse created ",where_clouse)


    delete_sql = "DELETE FROM `" +full_target_name+ "` WHERE "+tracking_date_column_name+"  >= '"+str(watermark_date)+"'"
    print("Delete SQL", delete_sql)
 
    job = target_conn.query(delete_sql)
    job.result()

    print("All Records Deleted >= ",watermark_date)
    print("Number of recorded deleted :",job.num_dml_affected_rows)
    
    return where_clouse


# =============================================================================
# Function To check if table exists 
# =============================================================================
def if_tbl_exists(db_connection, table_ref):
    from google.cloud.exceptions import NotFound
    
    client=bq_db_connect_ver2(db_connection)
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


# =============================================================================
#  MERGE Function,it perform the merge operation both step-1 delete , step-2 Insert
# =============================================================================

def merge_load_from_staging_to_final(country,target_sa_id,source_table_name):
   
    ( 
    	source_table_name, source_system_name, data_load_method,incremental_date_column, 
    	source_dataset, target_staging_dataset, target_prod_dataset, 
    	where_clause, merge_criteria_column, source_name, 
        etl_process_name, use_data_chunk_load, order_by_column_name,is_pii_table
    ) = config_extract(source_table_name)  
    
    print("Data Merge from STAGING to PRODUCTION started...")
    print("================================================")
    config=read_file_from_gcs(file_name ="sql_statements.yaml")
    merge_full_statement=config["sql_merge_statements"]
    
    # Getting Sql String from YAML file
    steps=merge_full_statement["MERGE_OPERATION"]
    for key in steps.keys():
        if key == "step_1_merge_delete":
            step_1_merge_delete_sql=steps[key]
                   
        elif key == "step_2_merge_insert":
            step_2_merge_insert_sql=steps[key]
     
    target_prod_dataset=target_prod_dataset+"_"+country.lower()
    target_staging_dataset=target_staging_dataset+"_"+country.lower()
    table_name=f"{source_system_name}_{source_table_name}"
    
    # Prepare join columns 
    join_clause=""
    for i, col in enumerate(merge_criteria_column.split(",")):
        if i==0:
            join_clause+=f"t.{col}=s.{col}"
        else:
            join_clause+=f" AND t.{col}=s.{col}"

    print(join_clause)    

    #Preparing sql 
    
    fmt_step_1_merge_delete_sql=step_1_merge_delete_sql.format(target_prod_dataset=target_prod_dataset, table_name=table_name,\
                                  incremental_date_column=incremental_date_column, merge_criteria_column=merge_criteria_column,\
                                   target_staging_dataset=target_staging_dataset,\
                                    join_clause=join_clause   )      
    
    print("Step-1 SQL:",fmt_step_1_merge_delete_sql)    
        
    fmt_step_2_merge_insert_sql=step_2_merge_insert_sql.format(target_prod_dataset=target_prod_dataset,table_name=table_name,\
                                    incremental_date_column=incremental_date_column,merge_criteria_column=merge_criteria_column,\
                                   target_staging_dataset=target_staging_dataset,\
                                    join_clause=join_clause   )   
    
    print("Step-2 SQL:",fmt_step_2_merge_insert_sql)

    target_connection=bq_db_connect_ver2(target_sa_id)
    staging_table_id=f"{target_staging_dataset}.{table_name}"
    prod_table_id=f"{target_prod_dataset}.{table_name}"
    deleted_row_count=0 
    inserted_row_count=0
    
    print("Cheking If table exists..")
        
    if if_tbl_exists(target_sa_id, prod_table_id):
        print("Table exists on production, performing Merge operation..")
        # Run merge Delete statement         
        delete_query_job= target_connection.query(fmt_step_1_merge_delete_sql) 
        delete_query_job.result()
        deleted_row_count=delete_query_job.num_dml_affected_rows
        print(f"Number of rows deleted:  {delete_query_job.num_dml_affected_rows} rows.")
        
        # Run merge Insert statement
        insert_query_job= target_connection.query(fmt_step_2_merge_insert_sql) 
        insert_query_job.result()
        inserted_row_count=insert_query_job.num_dml_affected_rows
        print(f"Number of rows Inserted: {insert_query_job.num_dml_affected_rows} rows.")
    
    # If table not exists then copy table from staging to production     
    else:
        print("Table not exists on production, seems first time load, Performing table copy")
        sql_table_copy=" CREATE OR REPLACE TABLE `" + prod_table_id + "` AS SELECT * FROM `" + staging_table_id +"`  "
        delete_query_job= target_connection.query(sql_table_copy) 
        delete_query_job.result()
        
        print("Table copied on production..")
        
    return deleted_row_count , inserted_row_count

# =============================================================================
#  MAIN function to load data 
# =============================================================================
def data_load_main(country, source_table_name):
    
    ( 
    	source_table_name, source_system_name, data_load_method,incremental_date_column, 
    	source_dataset, target_staging_dataset, target_prod_dataset, 
    	where_clause, merge_criteria_column, source_name, 
        etl_process_name, use_data_chunk_load, order_by_column_name, is_pii_table
    ) = config_extract(source_table_name)   
    
    start_ts = datetime.now(timezone.utc)
    print("Process started at :",start_ts)
    
    # !Getting connection info
    connection1= BaseHook.get_connection('khq_edl_service_acc')
    KHQ_SOURCE_SA_ID= connection1.login
    connection2= BaseHook.get_connection('cdp_etl_bq_service_acc')
    CDP_TARGET_SA_ID= connection2.login
    connection3= BaseHook.get_connection('cdp_pii_etl_bq_service_acc')
    CDP_PII_TARGET_SA_ID=connection3.login
    
    #Country filter code 
    if country=="FR":
        country_filter_code =" 'fr', 'france' "
         
    print("incremental_date_column",incremental_date_column)

    print("where 0: ", where_clause)
    if source_table_name=="V_L0GERP_XXINVS_MODEL_S_IF" \
        or source_table_name=="V_L0GERP_XXINVS_CURR_INV_S_IF"\
        or source_table_name=="V_L0GERP_XXTMS_PRICE_INFORMATION_S_IF" :
        where_clause=where_clause.replace("<country_code>","'LGEFS'")  
    
    else: 
        where_clause =where_clause.replace("<country_code>",country_filter_code)
    
    print("where 1: ", where_clause)
    target_staging_dataset = target_staging_dataset+f'_{country.lower()}'
    target_prod_dataset = target_prod_dataset+f'_{country.lower()}'
    
    #Connection change for PII tables
    if is_pii_table== "y":
        target_sa_id = CDP_PII_TARGET_SA_ID
    else:
        target_sa_id = CDP_TARGET_SA_ID
        
    source_conn=bq_db_connect_ver2(KHQ_SOURCE_SA_ID)
    
    try: 
        if data_load_method == "merge":
            target_dataset = target_staging_dataset
        else:
            target_dataset = target_prod_dataset
    
        full_source_name=source_dataset+"."+source_table_name
        full_target_name=target_dataset+"."+source_system_name+"_"+source_table_name+""
        target_table_id= full_target_name
        
        #Design the table schema 
        if if_tbl_exists(target_sa_id, target_table_id):
            schema=[]
        else:
            table = source_conn.get_table(full_source_name)
            schema=table.schema
            
            # Add Aditional column
            new_column_cdp_load_ts= ('cdp_load_ts', 'STRING', 'NULLABLE', None, (), None)
            new_column_cdp_anon_customer_id= ('cdp_anon_customer_id', 'STRING', 'NULLABLE', None, (), None)
            
            schema.append(bigquery.SchemaField(*new_column_cdp_load_ts))
            schema.append(bigquery.SchemaField(*new_column_cdp_anon_customer_id))
            
        job_config = bigquery.LoadJobConfig(
                schema=schema,
                )
        
        loaded_count=0
        print("where 2: ", where_clause)
        target_conn=bq_db_connect_ver2(target_sa_id)
        
     # =============================================================================
     # Increamental loading      
     # =============================================================================   
        if data_load_method == "merge":
        
            #Reset the WATERMARK 
            #---------------------
            if if_tbl_exists(target_sa_id, full_target_name):  
             
                where_clause=setup_watermark(source_system_name,target_sa_id,full_target_name,where_clause,incremental_date_column=incremental_date_column)
                
            if str(use_data_chunk_load).lower() == 'y': 
                loaded_count=bulk_chunk_data_load_bq(KHQ_SOURCE_SA_ID,target_sa_id,target_table_id,job_config,full_source_name
                                            ,where_clause,order_by_column_name)
            
            elif str(use_data_chunk_load).lower() == 'n':
                result_df=query_bq_source(full_source_name,source_conn,where_clause,is_partition=True)
                
                
                if len(result_df)>0:
                #Load data
                    load_bq_target(target_conn,result_df,target_table_id,job_config)
                    loaded_count=len(result_df)
                
            else:
                print("Missing Configuration for use_data_chunk_load column")
            
      # =============================================================================
      # Full table loading (Full Refresh Table)      
      # =============================================================================      
      
        elif data_load_method == "full-refresh":
            
            if if_tbl_exists(target_sa_id, target_table_id):
                truncate_sql=f"Truncate table  {target_table_id}"
                parent_job = target_conn.query(truncate_sql)
                rows_iterable = parent_job.result()
                print("Table truncated: ",target_table_id)
            
            else:
                print("First time table loading..")
            
            if str(use_data_chunk_load).lower() == 'y': 
                

                loaded_count=bulk_chunk_data_load_bq(KHQ_SOURCE_SA_ID,target_sa_id,target_table_id,job_config,full_source_name
                                                ,where_clause,order_by_column_name)
                
         
            elif str(use_data_chunk_load).lower() == 'n':
                result_df=query_bq_source(full_source_name,source_conn,where_clause,is_partition=False)
                
                if len(result_df)>0:
                #Load data
                    load_bq_target(target_conn,result_df,target_table_id,job_config)
                    loaded_count=len(result_df)
                
            else:
                print("Missing Configuration for use_data_chunk_load column")
                

        end_ts = datetime.now(timezone.utc)
        print("Process completed at :",end_ts)
        
        log(source_name,etl_process_name,country,full_source_name,full_target_name,loaded_count
            ,loaded_count,loading_status="Success",load_start_dt=start_ts,load_end_dt=end_ts
            ,source_system_name=source_system_name,data_load_method=data_load_method)  
        
        
    # =============================================================================
    #   Data MERGE Operation from Stging to Production       
    # =============================================================================
        if data_load_method == "merge":
            
            target_table_name=f"{source_system_name}_{source_table_name}"
            new_source_name =full_target_name
            new_target_name=f"eu-dxone-353411.{target_prod_dataset}.{target_table_name}"
        	
            print("Production data load process started at :",start_ts)
        	
        	## merge Operation
            delete_count,insert_count=merge_load_from_staging_to_final(country,target_sa_id,source_table_name)
        	
            end_ts = datetime.now(timezone.utc)
        	
        	## Logging
            log("hsad-staging","data-merge",country,new_source_name,new_target_name,delete_count,insert_count,loading_status="Success"
                ,load_start_dt=start_ts,load_end_dt=end_ts,source_system_name=source_system_name,data_load_method=data_load_method)      
        	
    
    except BaseException as exception:
        # print(logging.warning(f"Exception Desc: {exception}"))
        error_mesaage =str(logging.warning(f"Exception Desc: {exception}"))
        print(error_mesaage)
        log(source_name,etl_process_name,country,full_source_name ,full_target_name,loaded_count,loaded_count,loading_status="Fail"
            ,load_start_dt=start_ts,load_end_dt=end_ts,error_message=error_mesaage,source_system_name=source_system_name,data_load_method=data_load_method)

# =============================================================================
# Function to read data from BQ database 
# =============================================================================
def query_bq_source_metedata(full_source_name,connection_obj,where_clouse=" limit 1000"):
     sql_str="""
     SELECT    count(*),LOCALE_CODE FROM `"""+full_source_name+"` "  +" Group by LOCALE_CODE"

     print(sql_str)
     # Excute the SQL Job and convert to dataframe
     source_df = connection_obj.query(sql_str).to_dataframe()  
     print("Total No.of records extracted::",len(source_df.index))
     return source_df


# =============================================================================
# # Function to read product feed with link   
# =============================================================================
def read_feed_from_link(url=None,target_table_id=None ):
    
    # !Getting connection info

    
    connection2= BaseHook.get_connection('cdp_etl_bq_service_acc')
    CDP_TARGET_SA_ID= connection2.login
    
    connection3= BaseHook.get_connection('cdp_pii_etl_bq_service_acc')
    CDP_PII_TARGET_SA_ID=connection3.login
    
    if url== None :
        URL="https://www.semtrack.de/e?i=cdd888d96aa8539efdf2130c5d0d50193840f184"
    else:
        URL=url
    
    if  target_table_id == None:
        TARGET_TABLE_ID="eu-dxone-353411.ds_dev_stg_fr.feed_dynamix_product_info"
    else:
        TARGET_TABLE_ID=target_table_id
    
    # config=read_file_from_gcs()
    # target_sa_id = config['gcs_key_filepath']['target']
    
    target_conn=bq_db_connect_ver2(CDP_PII_TARGET_SA_ID)
    
    response=requests.get(URL)
    type(response)
    response_data=response.text
    string_io=io.StringIO(response_data)
    data_frame=pd.read_csv(string_io,sep=",")
    if if_tbl_exists(CDP_PII_TARGET_SA_ID, TARGET_TABLE_ID):
        truncate_sql=f"Truncate table  {TARGET_TABLE_ID}"
        parent_job = target_conn.query(truncate_sql)
        rows_iterable = parent_job.result()
        print("Table truncated: ",TARGET_TABLE_ID)
    
    else:
        print("First time table loading..")
    
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("availability", "STRING"),
        bigquery.SchemaField("brand", "STRING"),
        bigquery.SchemaField("condition", "STRING"),

    ])
    
# Loading table
    load_bq_target(target_conn,data_frame,TARGET_TABLE_ID,job_config)
 

