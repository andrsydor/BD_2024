from typing import List


class CSVDataManager:

    def __init__(self, pyspark_session, data_directory_path):
        self.__pyspark_session = pyspark_session
        self.__data_directory_path = data_directory_path

    def read(self, paths_in_data_directory, schema=None, header=True):
        assert isinstance(paths_in_data_directory, List)
        full_paths = [f'{self.__data_directory_path}{path}' for path in paths_in_data_directory]
        return self.__pyspark_session.read.csv(
            full_paths, schema=schema, header=header, quote='"', escape="\"", multiLine=True)

    def write(self, dataframe, path_in_data_directory):
        full_path = f'{self.__data_directory_path}{path_in_data_directory}'
        dataframe.write.csv(full_path, header=True)
