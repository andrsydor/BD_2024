
class Environment:
    # TODO: use environmental variables for this

    def __init__(self):
        self.__data_directory_path = "D:\\pp_git\\BD_2024\\data"
        # self.__data_directory_path = "/content/drive/MyDrive/big_data/data"
        self.__app_name = "BD_2024"
        self.__spark_master = "local"

    def data_directory_path(self):
        return self.__data_directory_path

    def app_name(self):
        return self.__app_name

    def spark_master(self):
        return self.__spark_master
