from log import  logger
import os
import pandas
import numpy
class TransformData:
    """
    class name: Transform Data
    description: this class is used for transforming data inside the file if required
    created by :Monojit Saha
    credit:Ineuron Intelligence
    version:1.0
    revision:none
    """
    def __init__(self):
        self.path = "training_good_&_bad_files_directory/good_raw_files"
        self.create_log = logger()
    def replace_missing_values(self):
        """
        function name:replace_missing_values
        description: this function replaces the missing values in the table with the value passed as a parameter.
        created by:Monojit Saha
        credit:Ineuron Intelligence
        version:1.0
        :param value:
        :return: none
        revision:none
        """
        try:
            for file in os.listdir(self.path):
                d = pandas.read_csv(self.path+"/"+file)
                d.fillna("Null",inplace=True)
                d.to_csv("training_good_&_bad_files_directory/good_raw_files/"+file,index=False)
            file_object=open('logs/Train/DATATRANSFORM.txt',"a+")
            self.create_log.writelog(file_object,"missing values replaced with NULL")
            file_object.close()
        except Exception as e:
            file_object=open("logs/Train/DATATRANSFORM.txt",'a+')
            self.create_log.writelog(file_object,"Exception: %s"%str(e))
            raise e

    def rename_columns(self):
        """
        function name:rename_columns
        description:used for renaming columns
        created by:Monojit saha
        version:1.0
        :return:none
        revision:none
        """
        try:
            for file in os.listdir("training_good_&_bad_files_directory/good_raw_files"):
                csv = pandas.read_csv("training_good_&_bad_files_directory/good_raw_files/"+file)
                print(csv.head(5))
                csv.rename(columns={'Unnamed: 0' : 'Wafer'},inplace=True)
                print(csv.head())
                csv.to_csv("training_good_&_bad_files_directory/good_raw_files/"+file,index = False)
            f=open('logs/Train/DATATRANSFORM.txt',"a+")
            self.create_log.writelog(f,"column renamed !!! ")
            f.close()
        except Exception as E:
            f=open('logs/Train/DATATRANSFORM.txt',"a+")
            self.create_log.writelog(f,"Exception : "+str(E))
            f.close()
            raise E

