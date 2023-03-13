import pandas as pd
import pickle

try:
    config_df = pd.read_csv("util/etl_config.csv")
    
    with open("util/config_df.pkl", 'wb') as f:
        pickle.dump(config_df, f)

except:
    config_df = pd.read_csv("etl_config.csv")
    
    with open("config_df.pkl", 'wb') as f:
        pickle.dump(config_df, f)


try:
    config_df = pd.read_csv("util/filename.csv")
    
    with open("util/filename.pkl", 'wb') as f:
        pickle.dump(config_df, f)

except:
    config_df = pd.read_csv("filename.csv")
    
    with open("filename.pkl", 'wb') as f:
        pickle.dump(config_df, f)
