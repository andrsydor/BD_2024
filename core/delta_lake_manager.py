from delta.tables import DeltaTable


class DeltaLakeManager:
    def __init__(self, pyspark_session, data_directory_path, deltalake_name):
        self.__pyspark_session = pyspark_session
        self.__data_directory_path = data_directory_path
        self.__deltalake_name = deltalake_name

    def __path_to_data(self, directory_in_deltalake):
        return f"{self.__data_directory_path}\\{self.__deltalake_name}\\{directory_in_deltalake}"
        # return f"{self.__data_directory_path}/{self.__deltalake_name}/{directory_in_deltalake}"

    def read(self, path_in_deltalake):
        full_paths = self.__path_to_data(path_in_deltalake)
        return self.__pyspark_session.read.format("delta").load(full_paths)

    def write(self, dataframe, path_in_deltalake, partition_column_name):
        save_path = self.__path_to_data(path_in_deltalake)
        dataframe.write.mode(saveMode="overwrite").format("delta").partitionBy(partition_column_name).save(save_path)

    def upsert_by_id(self, dataframe, path_in_deltalake):
        full_paths = self.__path_to_data(path_in_deltalake)
        delta_table = DeltaTable.forPath(self.__pyspark_session, full_paths)

        delta_table.alias("original") \
            .merge(
                dataframe.alias("new_data"),
                "original.Id = new_data.Id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
