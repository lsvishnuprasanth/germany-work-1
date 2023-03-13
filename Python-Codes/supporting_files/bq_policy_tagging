# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 13:37:47 2023

@author: Vishnu Prasanth
"""

import os
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import datacatalog_v1beta1
import pandas as pd
# import pickle


#Get all policy tags in the Taxonomy - "cdp_mask_pii_columns"

def get_policy_tags():
    
    print("Fetching Policy Tags ...")
    # os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    pt_name = []
    pt_disp_name = []
    policy_tag_dict = {}

    # Create a client
    pt_client = datacatalog_v1beta1.PolicyTagManagerClient()

    # Initialize request argument(s)
    request = datacatalog_v1beta1.ListPolicyTagsRequest(
        parent="projects/project_id/locations/europe-west3/taxonomies/5056668844747900290",
    )

    # Make the request
    page_result = pt_client.list_policy_tags(request=request)
   
    # Handle the response
    for response in page_result:
        # response_str = response.display_name+','+ response.name
        pt_disp_name.append(response.display_name)
        pt_name.append(response.name)
        
    policy_tag_dict = dict(zip(pt_disp_name,pt_name))
    # print(policy_tag_dict)
    return policy_tag_dict


# Get all PII Datasets
# Assign Label of Dataset " pii = yes " to be considered as PII

def get_pii_datasets(bq_client):
    
    print("Fetching PII Datasets ...")
    # os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    #bq_client = bigquery.Client()
    dataset_list =  list(bq_client.list_datasets())
    pii_datasets = []
    non_pii_datasets = []

    for dataset in dataset_list:
        # print(dataset.dataset_id)
        ds_id = bq_client.get_dataset(dataset.dataset_id)
        labels =  ds_id.labels
        if labels:
            for label, value in labels.items():
                if label =="pii" and value == "yes":       
                    pii_datasets.append(ds_id.dataset_id)
                    # print("\t{}: {}".format(label, value))
        else:
            non_pii_datasets.append(ds_id.dataset_id)
            # print("\tDataset has no labels defined.")
    print(pii_datasets)
    return pii_datasets


# Get all PII tables in PII datasets
# Assign Label of Table " pii = yes " to be considered as PII or Table name like "x2003243"

def get_pii_tables(pii_datasets,bq_client):
    print("Fetching PII Tables ...")
    # os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    pii_tables = []
    non_pii_tables = []
    #bq_client = bigquery.Client()
    for dataset in pii_datasets:
        # print(f"\nDataset ID : {dataset}\n")
        # print("Tables:\n")
        tables = list(bq_client.list_tables(dataset))  # Make an API request(s).
        if tables:
            for table in tables:
                # print("\t{}".format(table.table_id))
                tbl_id =  table.table_id.lower()
                tbl_label = table.labels
                if tbl_label:
                    for label, value in tbl_label.items():
                        if (label =="pii" and value == "yes"):
                        # if "x2003243" in tbl_id:
                            # print(1)
                            tablename = dataset + "." + table.table_id
                            pii_tables.append(tablename)
                elif "x2003243" in tbl_id:
                    tablename = dataset + "." + table.table_id
                    pii_tables.append(tablename)
                else:
                    # print("No pii tables found")      
                     non_pii_tables.append(table.table_id)
        else:
            print("\tThis dataset does not contain any tables.")
    
    return pii_tables


#Function to ignore tables which are already policy tagged

def policy_tag_check(pii_tables, pii_df,bq_client):
    pii_table_tagged = []
    pii_table_not_tagged = []
    #bq_client = bigquery.Client()
    for table in pii_tables:
        pii_cols = []
        table_name = bq_client.get_table(table)
        table_schema = table_name.schema
        for cols in table_schema:
            if cols.policy_tags != None:
                # print("Policy Tags assigned", cols.name)
                pii_cols.append(cols.name)
            
                # print("Policy Tags not assigned" , cols.name)
                
        pii_df_series = pii_df.groupby(['Table'])['Column_Name'].count()
        pii_df_groupby = pd.DataFrame({'Table' : pii_df_series.index, 'Count' : pii_df_series.values})
        
        table_to_check = table[:table.find("_X2003243")]
        
        if table_to_check.split('.')[1] in pii_df_groupby['Table'].unique():
            pii_cols_cnt =  pii_df_groupby[pii_df_groupby['Table'] == table_to_check.split('.')[1]]['Count'].item()
        else:
            pii_cols_cnt = 0
        
        if len(pii_cols) > 0 and len(pii_cols) == pii_cols_cnt:
            pii_table_tagged.append(table)
            
        else:
            pii_table_not_tagged.append(table)
            
    print(f"{len(pii_table_tagged)} Table(s) already tagged ! Table List : {pii_table_tagged}")
    print(f"{len(pii_table_not_tagged)} Table(s) not tagged ! Table List : {pii_table_not_tagged}")
    return pii_table_not_tagged
