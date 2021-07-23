from raw_file_validation import RawDataValidation
from DataBaseOperation import DataBaseOperation
from Data_Transform.data_transform import TransformData
import log


class TrainValidateInsert:
    def __init__(self, path, schema_path):
        self.schema_path = schema_path
        self.path = path
        self.data_base = 'Training'
        self.RawFileValidate = RawDataValidation(self.path, self.schema_path)
        self.TransformData = TransformData()
        self.DataBaseOperation = DataBaseOperation()
        self.create_log=log.logger()

    def validate_transform_insert(self):
        try:
            length_of_date_stamp, length_of_time_stamp, number_of_columns, column_name \
            = self.RawFileValidate.get_values_from_schema()
            regex = self.RawFileValidate.make_regex_pattern()
            #self.RawFileValidate.make_raw_file_directory()
            validate_file_name = self.RawFileValidate.file_name_validation(length_of_date_stamp, length_of_time_stamp, regex)
            self.RawFileValidate.validate_column_length(number_of_columns)
            self.RawFileValidate.validate_missing_values_in_entire_column()
            self.RawFileValidate.move_bad_files_to_archive()
            # Transforming files
            column_rename = self.TransformData.rename_columns()
            missing_values = self.TransformData.replace_missing_values()
            # insert data to db
            create_table = self.DataBaseOperation.createtable(self.data_base, column_name)
            insert = self.DataBaseOperation.InsertTableData(self.data_base)
            export = self.DataBaseOperation.ExportFromDbToCsv()

            file_obj = open("logs/Train/train_validate_insert.txt", "a+")
            self.create_log.writelog(file_obj, "validation,transformation,insertion done sucessfully !!!")
            file_obj.close()

        except Exception as e:
            file_obj = open("logs/Train/train_validate_insert.txt", "a+")
            self.create_log.writelog(file_obj ,"Error!!! %s" % str(e))
            file_obj.close()
            raise e



