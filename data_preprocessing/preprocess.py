import pandas as pd
from sklearn.impute import KNNImputer
import numpy as np


class data_preprocessing:
    """
    class name :data preprocessing
    description: this class is used for all kinds preprocessing
    version:1.0
    """
    def __init__(self, data, file_obj, log_object):
        self.file_obj = file_obj
        self.log_object = log_object
        self.dataframe = data

    def remove_columns(self, column_name):
        """
        description:this method is used to remove column from a table
        version:1.0
        revision: none
        :param column_name:
        :return: updated table
        """
        self.column_name = column_name
        try:
            self.updated = self.dataframe.drop(self.column_name, axis=1)  # drop column from the dataframe
            self.log_object.writelog(self.file_obj, "dropped {} column from the table".format(self.column_name))
            return self.updated
        except Exception as e:
            self.log_object.writelog(self.file_obj,
                                     "error in the remove column method of data preprocessing %s" % str(e))
            self.log_object.writelog(self.file_obj, "remove column unsuccessful due to %s" % str(e))
            raise e

    def separate_features_label(self, dataframe, column_name):
        """
        method name:separate_features_labels
        description:split features and labels from the table
        :return: label and features
        """

        self.dataframe = dataframe
        try:
            self.log_object.writelog(self.file_obj, "Entered the separate_features_label method")
            self.X = self.dataframe.drop(labels=column_name, axis=1)
            self.y = self.dataframe[column_name]
            return self.X, self.y
        except Exception as e:
            self.log_object.writelog(self.file_obj, "error occurred in separate_features_and_labels " + str(e))
            raise  Exception()
    def is_null_present(self,data):
        self.log_object.writelog(self.file_obj,"Enter the is_null_present_method of preprocess class")
        self.null_present = False
        try:
            self.null_counts=data.isna().sum()
            for col in self.null_counts:
                if (col>0):
                    self.null_present=True
                    break
                if(self.is_null_present):
                    dataframe_with_null=pd.DataFrame()
                    dataframe_with_null["columns"]=data.columns()
                    dataframe_with_null["count"]=np.asarray(self.null_count)
                    dataframe_with_null.to_csv("preprocessing_data/null_values.csv")
                    self.log_object.writelog(self.file_obj,"finding missing values successful. Data written to the null_values file")
                    return  self.null_present
        except Exception as e:
            self.log_object.writelog(self.file_obj,"Exception occured in is_null_present method of preprocess class")
            self.log_object.writelog(self.file_obj,"Exited the is_null_present method of preprocess class")
            self.file_obj.close()
            raise Exception()


    def is_null_present(self,data):
        self.fileobject=self.file_obj
        self.log_object.writelog(self.fileobject,"Entered the is_null_present method")
        self.data=data
        self.is_null_present=False
        self.null_counts=self.data.isna().sum()
        try:
            for i in self.null_counts:
                if i>0:
                    self.is_null_present=True
                    break

            if(self.is_null_present):
                dataframe_with_null=pd.DataFrame()
                dataframe_with_null["columns"]=data.columns
                dataframe_with_null['missind values coumt']=np.asarray(data.isna().sum())
                dataframe_with_null.to_csv("Preprocessing_data/null_values.csv")
                self.log_object.writelog(self.fileobject,"Finding missind values is a success ,data written in null values file exited the is_null_present_method of preprocess class")
                return  self.is_null_present

        except Exception as E:
            self.log_object(self.fileobject,"Exception occured in is_null_present method of preprocessing class  Exception message :"+str(E))
            self.log_object(self.file_object,"Exited the is_null_present method of preprocess class")
            return  Exception()

    def impute_missing_values(self,dataframe):
        self.log_object.writelog(self.file_obj,"Enter the impute_missing_values method of data_preprocessing class")
        self.data = dataframe
        try:
            imputer = KNNImputer(n_neighbors=3,missing_values=np.nan)
            self.new_array=imputer.fit_transform(self.data)#impute the missing values
            self.new_data=pd.DataFrame(data=self.new_array,columns=self.data.columns)
            self.log_object.writelog(self.file_obj,"Imputing missing values successful ,Exted the 'impute_missing_values' method of preprocess class")
            return self.new_data
        except Exception as e:
            self.log_object.writelog(self.file_obj,"Exception occured in the impute_missing_values method of preprocess class ,Error : %s"%str(e))
            self.log_object.writelog(self.file_obj,"Exited impute_missing_values  method of preprocess class ")
            raise  Exception()

    def get_columns_with_zero_std_deviation(self,data):
        self.log_object.writelog(self.file_obj,"Entered the get_columns_with_zero_std_deviation method of preprocess class ")
        self.columns=data.columns
        self.data_n=data.describe()
        self.col_to_drop=[]
        try:
            for col in self.columns:
                if(self.data_n[col]['std'] == 0):
                    self.col_to_drop.append(col)
            self.log_object.writelog(self.file_obj,"column search for zero standard deviation successful .Exit the get_columns_with_zero_std_deviation method of the preprocess class.")
            return  self.col_to_drop
        except Exception as r:
            self.log_object.writelog(self.file_obj,"Exception occured at get_columns_with_zero_std_deviation. Exceptio %s"%str(r))
            self.log_object.writelog(self.file_obj,"Exited the get_columns_with_zero_std_deviation method of preprocess")
            raise Exception()

















