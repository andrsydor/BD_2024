
def write_initial_deltalake(deltalake_manager, dataframes_by_name, partition_column_name):
    for name, df in dataframes_by_name.items():
        deltalake_manager.write(df, name, partition_column_name)
    

def upsert_datalake(deltalake_manager, dataframes_by_name):
    for name, df in dataframes_by_name.items():
        deltalake_manager.upsert_by_id(df, name)
