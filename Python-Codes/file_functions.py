# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 16:56:57 2022

@author: Vishnu Prasanth
"""
from airflow.hooks.base_hook import BaseHook
import os
from datetime import datetime, timezone, timedelta
from google.cloud import storage
from google.cloud import bigquery
import pickle
from util.functions import query_bq_source,load_bq_target,log,read_file_from_gcs, bq_db_connect_ver2

#Function to load data from GCS to Staging Temp


#def config_extract_sftp(country, table_name):
def config_extract_sftp(table_name):
    
    print("Reading Sftp Config File...")
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    bucket_name="europe-west3-composer-airfl-9c9a4990-bucket"
    blob_name="dags/util/sftp_config_df.pkl" 
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    with blob.open("rb") as f:
        config_df = pickle.load(f)
        
    #df_fin = config_df[(config_df.country==country) & (config_df.table_name==table_name)]
    
    df_fin = config_df[(config_df.table_name==table_name)]
    
    if len(df_fin) != 1:
        print("Please make sure the dag table name and country exists in config file")
        raise Exception("Given country and table name not found in config, Please check configuration file!")
    
    return df_fin.values[0]


def load_to_bq_temp(uri,target_stg_temp_dataset,gcs_file_name,bq_client,country):
    
    print('\n*********** Data Loading from GCS to Temp Dataset Started *********** \n')  
    print('Reading File from Location :', uri)   
    target_temp_table_name = target_stg_temp_dataset +'.'+ gcs_file_name.replace(country+'/','').replace('.csv', '')   
    print('\nTarget TableName  in BigQuery : ', target_temp_table_name)    
    print("\nDefining BigQuery Job Config .... ")
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
    
    print('\nLoading into {} temp table'.format(target_temp_table_name))
    
    load_job = bq_client.load_table_from_uri(
          uri, target_temp_table_name , job_config=job_config
      )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    target_temp_table = bq_client.get_table(target_temp_table_name)  
    
    
    
    labels = {"pii":"yes"}
    
    target_temp_table.labels = labels
    
    target_temp_table = bq_client.update_table(target_temp_table, ["labels"])
    
    print("Label added...")
    
    #print(target_temp_table.schema, type(target_temp_table.schema))
    
    print('\nLoading Complete ... !')   
    print("\nLoaded {} rows.\n".format(target_temp_table.num_rows))   
    
    return target_temp_table.num_rows

#Function to load data from Staging Temp to Staging

def load_to_bq_stg(final_table,stg_temp_table_name,bq_client,gcs_file_name):
    
    print('\n*********** Data Loading from Temp Dataset to Staging Dataset Started *********** \n')
    print("\nLoading data from Staging temp to Staging Dataset \n")
    
    source_table = bq_client.get_table(stg_temp_table_name) 
    
    tmp = [col.name for col in source_table.schema]
    
    ins_table_cols = ", ".join(tmp)
    print(ins_table_cols)
    
    src_tmp = []
    for col in source_table.schema:
        if col.field_type != 'STRING':
            src_tmp.append(f"cast({col.name} as string) {col.name}")
        else:
            src_tmp.append(col.name)        

    sel_table_cols = ", ".join(src_tmp)
    
    additional_cols = 'sftp_file_name, eu_load_date, eu_load_ts,eu_created_by, '
    
    additional_col_values = f""" '{gcs_file_name}' as sftp_file_name, current_date eu_load_date,current_datetime eu_load_ts, 'SFTP' eu_created_by, """
    
    insert_query = f""" 
    INSERT INTO `{final_table}`({additional_cols}{ins_table_cols})
    SELECT {additional_col_values} {sel_table_cols}
    FROM
    `{stg_temp_table_name}`;
    """
    
    print('\n Insert Query : \n {} \n'  .format(insert_query))
    
    insert_query_job = bq_client.query(insert_query)
    
    insert_query_job.result()
    
    inserted_rows = insert_query_job.num_dml_affected_rows
    
    total_rows  = bq_client.get_table(final_table) 
    
    print('\n*********** Data Loading Info *********** ')
    
    loading_info = f""" 
    No. Of Records Inserted : {inserted_rows}
    Total Records in the Table : {total_rows.num_rows}
    """
    print(loading_info)      

    print('\nLoading Complete ... !')
    
    return total_rows.num_rows
    

#Function to Archive the file    
def archive_file(gcs_client,bucket_name,source_bucket,gcs_file_name,country):
    # Moving the File to Archive Folder
    print('\n*********** Archiving the File  *********** \n')
    
    destination_bucket  = gcs_client.get_bucket(bucket_name)   
    dest_blob = source_bucket.get_blob(gcs_file_name)
    print(dest_blob)
    
    dest_blob_name = dest_blob.name.replace(country+'/', '')                    

    print("\nMoving the File : {} to Archive".format(dest_blob_name))
            
    new_file_name = f'{country}/{country}_Archive/' + dest_blob_name
            
    destination_bucket.rename_blob(dest_blob, new_file_name)
            
    print('\nFile moved to Archive ...')

#################################################################################
# Main function
#################################################################################       
def sftp_data_load_main(country, table_name,i):
        
    (
        bucket_name, source_system_name, target_stg_temp_dataset,
        target_stg_dataset, table_name, source_name,etl_process_name
    )= config_extract_sftp(table_name)

    start_ts = datetime.now(timezone.utc)
    print("Process started at :",start_ts)

    ##########  This is for airflow connection ############################    
    # config=read_file_from_gcs()
    # key_filepath=config['gcs_key_filepath']['target']
    ######################################################################
    
    # Connecting to BQ
    
    connection2= BaseHook.get_connection('cdp_etl_bq_service_acc')
    CDP_TARGET_SA_ID= connection2.login

    gcs_client = storage.Client()
    bq_client = bq_db_connect_ver2(CDP_TARGET_SA_ID)

    # GCS Bucket Configurations

    bucket_name = bucket_name # 'eu-dxone-decrypted-files'
    folder = country+"/" # 'FR/'

    # Defining Target Bigquery Tables
    source_system_name = source_system_name # 'EMPL'
    target_stg_temp_dataset = target_stg_temp_dataset # 'ds_dev_stg_temp'
    target_stg_dataset = target_stg_dataset + f"_{country.lower()}" #'ds_dev_stg_fr' 
    #table_name = 'TB_MP_COM_CODE_DETAIL'
    final_table = target_stg_dataset + '.EMPL_' + table_name
    
    print('\n*********** Configuration Details *********** ')
    config_details = f""" 
    Bucket Name: {bucket_name}
    Folder Name: {folder}
    Table Name: {table_name}
    Staging Temp Dataset: {target_stg_temp_dataset}
    Staging Dataset: {target_stg_dataset}
    Final Table: {final_table} 
    """
    print(config_details)

    #Date Conversion for reading file from GCS
    
    # TODO: This part will fail when we work on past date data.
    #today = datetime.today().date()
    today = i # passing date from main function to fix past date data
    date_suffix = today.strftime('%Y%m%d')
    
    #print(' Today\'s Date :' , today)

    #Changing the dateformat to YYYYMMDD to read file from GCS
 
    #print(" Date Suffix for the file in GCS : ", date_suffix)

    #Creating File Name 
    print('\n*********** File Information ***********')
    
    gcs_file_name = country + '/' + table_name + '_'+country+'-'+ date_suffix + "00.csv"
    uri = "gs://"+ bucket_name + '/' + gcs_file_name
    
    
    file_info = f""" 
    File to search in GCS : {gcs_file_name}
    File Location : {uri} 
    """
    
    print(file_info)
    
    stg_temp_table_name = target_stg_temp_dataset +'.'+ gcs_file_name.replace(country+'/','').replace('.csv', '')
    
    stg_temp_table_info = f"""
    Staging Temp Table Created : {stg_temp_table_name} """
       
    print(stg_temp_table_info)
    
    print('\n*********** Checking File Availability *********** \n')

    source_bucket = gcs_client.bucket(bucket_name)

    file_check = storage.Blob(bucket = source_bucket, name = gcs_file_name).exists(gcs_client)

    if file_check == True:
        print("\033[2;30;42m File Found...  \033[0;0m \n")
        
        src_cnt = load_to_bq_temp(uri,target_stg_temp_dataset,gcs_file_name,bq_client,country)
               
        final_tbl_cnt = load_to_bq_stg(final_table,stg_temp_table_name,bq_client,gcs_file_name)
        
        archive_file(gcs_client,bucket_name,source_bucket,gcs_file_name,country)
        
        print('\n*********** Workflow Completed  *********** \n')
        end_ts = datetime.now(timezone.utc)
        print("Process completed at :",end_ts)
        
        log(source_name,etl_process_name,country,gcs_file_name ,final_table,src_cnt,final_tbl_cnt,loading_status="Success",load_start_dt=start_ts,load_end_dt=end_ts,source_system_name=source_system_name)     
        
    else:
        print("\033[2;31;47m File Not Found !  \033[0;0m ")
        print('\n*********** Workflow Completed  *********** \n')
        end_ts = datetime.now(timezone.utc)
        print("Process completed at :",end_ts)
        
        log(source_name,etl_process_name,country,gcs_file_name ,final_table,0,0,loading_status="No File",load_start_dt=start_ts,load_end_dt=end_ts,source_system_name=source_system_name)     
    
    return file_check



def sftp_file_check(country, table_name):
    
    bucket_name = 'eu-dxone-decrypted-files'
    
    #folder = country +"/"
    
    gcs_client = storage.Client()
    
    # TODO: This part will fail when we work on past date data.
    today = datetime.today().date()
    date_suffix = today.strftime('%Y%m%d')

    #Creating File Name 
    
    gcs_file_name = country + '/' + table_name + '_'+country+'-'+ date_suffix + "00.csv"
    # uri = "gs://"+ bucket_name + '/' + gcs_file_name
     
    # file_info = f""" 
    # File to search in GCS : {gcs_file_name}
    # File Location : {uri} 
    # """
    
    # print(file_info)   

    source_bucket = gcs_client.bucket(bucket_name)

    file_check = storage.Blob(bucket = source_bucket, name = gcs_file_name).exists(gcs_client)

    if file_check == True:        
        status = country + '-' + table_name + '-' + 'Yes'
        print("File Found ! ")     
                
    else:
        status = country + '-' + table_name + '-' + 'No'
        print("File Not Found ! ")
                   
    return status
