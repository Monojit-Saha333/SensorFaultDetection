"""
this package will be used for reading data from the clean input file
version:1.0
"""
import pandas

class Data_Getter:
    """
    :description: we load the data from the source
    :raises: exception
    :version: 1.0
    """
    def __init__(self, file_obj, logger_obj):
        self.input_file = "trainingfilefromdb/input.csv"
        self.log_obj = logger_obj
        self.file_object = file_obj

    def load_data(self):
        """
        method name:load data
        description:this method is used to get data into a pandas dataframe from the source
        revision:none
        :return: pandas dataframe
        """
        try:
            #file_obj=open("logs/train/ModelTraining.txt", "a+")
            self.log_obj.writelog(self.file_object, "Starting model training")
            #file_obj.close()
            self.data = pandas.read_csv(self.input_file)
            return self.data
        except Exception as e:
            #file_obj = open("logs/train/ModelTraining.txt", "a+")
            self.log_obj.writelog(self.file_object, "error while loading data in the load_data method of Data_Getter class : %s" % str(e))
            self.log_obj.writelog(self.file_object, "data loading unsuccessful due to {}".format(e))
            #file_obj.close()
            raise e


