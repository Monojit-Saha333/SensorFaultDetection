import json
import os
from os import listdir
import shutil
from datetime import datetime
import re
import pandas as pd
import log


class RawDataValidation:
    """
    created by:Monoit Saha
    description:this class is used for validating files based on file name , number of columns, creating directory for
    storing good and bad files
    version:1.0
    credit:Ineuron Intelligence
    """

    def __init__(self, path, schema_path):
        self.schema_path = schema_path
        self.training_batch_dir = path
        self.create_log = log.logger()

    def get_values_from_schema(self):
        """
        created by :Monojit Saha
        credit : Ineuron Intelligence
        description : this function is used for getting all the values from the training schema
        return : Lenght_of_date_stamp,Lenght_of_time_stamp,Number_of_Columns,Column_name
        version:1.0
        revision:none
        raise value error,key error,exception
        """
        file_obj = open("logs/Train/schemavalidation.txt", "a+")
        self.create_log.writelog(file_obj, "schema validation started!!!")
        file_obj.close()
        try:
            with open(self.schema_path) as f:
                schema = json.load(f)
                f.close()
                sample_file_name = schema['SampleFileName']
                lenght_of_date_stamp = schema['LengthOfDateStampInFile']
                lenght_of_time_stamp = schema['LengthOfTimeStampInFile']
                number_of_columns = schema['NumberofColumns']
                column_name = schema['ColName']
                f.close()
                file_obj = open("logs/Train/schemavalidation.txt", "a+")
                self.create_log.writelog(file_obj, "schema loaded successfully!!! " +
                                         "\n Sample_File_Name : " + str(sample_file_name)
                                         + "\n Length_of_date_stamp : " + str(lenght_of_date_stamp)
                                         + "\n Length_of_time_stamp : " + str(lenght_of_time_stamp)
                                         + "\n Number_of_columns : " + str(number_of_columns))
                file_obj.close()

        except ValueError:
            file_obj = open("logs/Train/schemavalidation.txt", "a+")
            self.create_log.writelog(file_obj, "value error:value not found in the specified key")
            file_obj.close()
            raise ValueError
        except KeyError as K:
            file_obj = open("logs/Train/schemavalidation.txt", "a+")
            self.create_log.writelog(file_obj, "key error : key not found inside the schema %s" % str(K))
            file_obj.close()
            raise KeyError
        except Exception as e:
            file_obj = open("logs/Train/schemavalidation.txt", "a+")
            self.create_log.writelog(file_obj, str(e))
            file_obj.close()
            raise e

        return lenght_of_date_stamp, lenght_of_time_stamp, number_of_columns, column_name

    def make_regex_pattern(self):
        """
        created by:Ineuron Intelligence
        description:this method used to make a regex pattern for file name validation
        return: regex pattern
        version:1.0
        revision:none
        """
        regex = "['Wwafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def make_raw_file_directory(self):
        """
        created by:Monojit Saha
        description:creates the raw directory for storing bad and good data files
        return: none
        version:1.0
        """
        fileobj = open("logs/Train/GeneralLog.txt", "a+")
        self.create_log.writelog(fileobj, "making raw file directory !!!")
        fileobj.close()
        try:
            fileobj = open("logs/Train/GeneralLog.txt", "a+")
            path = os.path.join('training_good_&_bad_files_directory/good_raw_files')
            if not os.path.isdir(path):
                x = os.mkdir(path)
                self.create_log.writelog(fileobj, "good_raw_files directory created for storing good raw files")

            path = os.path.join('training_good_&_bad_files_directory/bad_raw_files')
            fileobj = open("logs/Train/GeneralLog.txt", "a+")
            if not os.path.isdir(path):
                x = os.mkdir((path))
                self.create_log.writelog(fileobj, "bad_raw_files directory created for stored bad  files")
        except OSError as E:
            fileobj = open("logs/Train/GeneralLog.txt", "a+")
            self.create_log.writelog(fileobj, "Os Error : Error while creating directory  :: %s" % str(E))
            fileobj.close()

    def file_name_validation(self, length_of_date_stamp, Length_of_time_stamp, regex):
        """
        created by:Monojit Saha
        description: this function is used for validating the name of the files .
        :param length_of_date_stamp:
        :param Length_of_time_stamp:
        :param regex:
        :return: none
        :raises: os error
        version:1.0
        revision:none
        """
        fileobj = open("logs/Train/NameValidation.txt", "a+")
        self.create_log.writelog(fileobj, "start file name validation!!!")
        fileobj.close()
        try:
            self.delete_existing_good_raw_folder()
            self.delete_existing_bad_raw_folder()
            self.make_raw_file_directory()
            goodfilecount = 0
            BadFileCount = 0
            files = [i for i in os.listdir(self.training_batch_dir)]
            for file in files:
                print(file)
                if (re.match(regex, file)):
                    split_dot = re.split('.csv', file)
                    split_dot = re.split('_', split_dot[0])
                    # print(split_dot)
                    # print(len(split_dot[1]))
                    # print(len(split_dot[2]))
                    if (length_of_date_stamp == len(split_dot[1]) and Length_of_time_stamp == len(split_dot[2])):
                        if not os.path.isfile('training_good_&_bad_files_directory/good_raw_files/' + file):
                            shutil.copyfile('training_batch_files/' + file,
                                            'training_good_&_bad_files_directory/good_raw_files/' + file)
                            goodfilecount += 1
                        else:
                            print(" already exist in the good_raw_files directory")
                else:
                    if not os.path.isfile('training_good_&_bad_files_directory/bad_raw_files/' + file):
                        shutil.copyfile('training_batch_files/' + file,'training_good_&_bad_files_directory/bad_raw_files/' + file)
                        BadFileCount += 1
                    else:
                        print(file + "already exist in the bad_raw_folder directory")
            fileobj = open("logs/Train/NameValidation.txt", "a+")
            self.create_log.writelog(fileobj, "No of files copied to bad_raw_files folder : %s" % str(BadFileCount))
            self.create_log.writelog(fileobj, "No of files copied to good_raw_files folder : %s" % str(goodfilecount))
            fileobj.close()
        except OSError as e:
            fileobj = open("logs/Train/NameValidation.txt", "a+")
            self.create_log.writelog(fileobj, "OSError : %s" % str(e))
            fileobj.close()
            raise OSError

    def validate_column_length(self, column_length):
        """
        created by: monojit saha
        description : this function checks the number of columns present inside the file and rejects the file if the
        number of columns is not equal to the column length specified in json file and store it in bad_raw_files folder
        else the files are stored in good raw folders
        version:1.0
        revision:none
        :param column_length:
        :return:none
        """
        fileobj = open("logs/Train/NameValidation.txt", "a")
        self.create_log.writelog(fileobj, "start column length validation!!!")
        fileobj.close()
        try:
            files = [i for i in os.listdir('training_good_&_bad_files_directory/good_raw_files')]
            BadFileCount = 0
            for file in files:
                csv = pd.read_csv("training_good_&_bad_files_directory/good_raw_files/" + file)
                shape = csv.shape
                if (shape[1] != column_length):
                    shutil.move("training_good_&_bad_files_directory/good_raw_files/" + file,
                                'training_good_&_bad_files_directory/bad_raw_files/' + file)
                    BadFileCount += 1

            fileobj = open("logs/Train/NameValidation.txt", "a+")
            self.create_log.writelog(fileobj,
                                     "No of files moved to bad_raw_files folder after column length validation : %s" % str(
                                         BadFileCount))
            fileobj.close()
        except OSError as E:
            fileobj = open("logs/Train/NameValidation.txt", "a+")
            self.create_log.writelog(fileobj, "OS Error : %s" % str(E))
            fileobj.close()
            raise OSError
        except Exception as e:
            fileobj = open("logs/Train/NameValidation.txt", "a")
            self.create_log.writelog(fileobj, "key error %s" % str(e))
            fileobj.close()
            raise e

    def validate_missing_values_in_entire_column(self):
        """
        created by:Monojit Saha
        description:checks whether the entire column is empty or not and based on that the files are moved to
        good_raw_files and bad_raw_files folder
        version:1.0
        revision:none
        :return: none
        """
        try:
            files = [x for x in listdir("training_good_&_bad_files_directory/good_raw_files")]
            for file in files:
                csv = pd.read_csv("training_good_&_bad_files_directory/good_raw_files/" + file)
                count = 0
                for column in csv.columns:
                    if (len(csv[column]) - csv[column].count() == len(csv[column])):  # column is empty
                        count += 1
                        shutil.move("training_good_&_bad_files_directory/good_raw_files/" + file,
                                    "training_good_&_bad_files_directory/bad_raw_files/" + file)
                        break
                if (count == 0):
                    csv.rename(columns={"unnamed : 0": "wafer"}, inplace=True)
        except Exception as e:
            print(e)

    def delete_existing_good_raw_folder(self):
        """
        created by: monojit saha
        description : deletes the good_raw_files folder if it exists
        version:1.0
        revision:none
        :return:none
        """
        path = "training_good_&_bad_files_directory"
        try:
            if os.path.isdir("training_good_&_bad_files_directory/good_raw_files"):
                shutil.rmtree("training_good_&_bad_files_directory/good_raw_files")
        except Exception as e:
            print(e)

    def delete_existing_bad_raw_folder(self):
        """
        created by:Monojit Saha
        description:deletes the bad_raw_files folder
        version:1.0
        revisions:none
        :return:
        """
        try:
            if os.path.isdir("training_good_&_bad_files_directory/bad_raw_files"):
                shutil.rmtree("training_good_&_bad_files_directory/bad_raw_files")
        except Exception as e:
            print(e)

    def move_bad_files_to_archive(self):
        """
        created by:Monojit Saha
        description:move files from the bad_file_folder to archives
        revisions:none
        :return: 
        """
        date = datetime.now().date()
        time = datetime.now().strftime("%H-%M-%S")
        destination_folder_name = "badfiles__" + str(date) + "__" + str(time)

        try:
            if destination_folder_name not in listdir("training_archives"):
                os.mkdir("training_archives/" + destination_folder_name)
                count = 0
                no_of_files = len(listdir('training_good_&_bad_files_directory/bad_raw_files'))
                for file in listdir('training_good_&_bad_files_directory/bad_raw_files'):
                    # file_name=file+str(date)+str(time)
                    if file not in listdir("training_archives/" + destination_folder_name):
                        shutil.move("training_good_&_bad_files_directory/bad_raw_files/" + file,
                                    "training_archives/" + destination_folder_name + "/" + file)
                    count += 1
                fileobj = open("logs/Train/GeneralLog.txt", "a+")
                self.create_log.writelog(fileobj,
                                         "%s" % (str(count)) + " of %s" % str(no_of_files) + "transfered to archives")
                fileobj.close()
        except OSError as O:
            fileobj = open("logs/Train/GeneralLog.txt")
            self.create_log.writelog(fileobj, "OS Error : %s" % str(O))
            fileobj.close()
        except Exception as E:
            fileobj = open("logs/Train/GeneralLog.txt", "a+")
            self.create_log.writelog(fileobj, "Exception : %s" % str(E))
            fileobj.close()
            raise E
