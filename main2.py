from DataBaseOperation import  DataBaseOperation

from raw_file_validation import RawDataValidation
from  Data_Transform.data_transform import TransformData
data_val=RawDataValidation("training_batch_files","schema_training.json")

Length_of_date_stamp_in_file,Length_of_time_stamp_in_file,Number_of_columns,Column_name,=data_val.get_values_from_schema()

#print(Length_of_date_stamp_in_file)
#print(Length_of_time_stamp_in_file)
#print(Number_of_columns)
regex=data_val.make_regex_pattern()
#print(regex)
data_val.file_name_validation(Length_of_date_stamp_in_file,Length_of_time_stamp_in_file,regex)
data_val.validate_column_length(Number_of_columns)
#data_val.make_raw_file_directory()
data_val.validate_missing_values_in_entire_column()
data_val.move_bad_files_to_archive()

transform=TransformData()
transform.replace_missing_values()
transform.rename_columns()

dbobj=DataBaseOperation()
#dbobj.ConnectToDB("Training")
dbobj.createtable("Training",Column_name)
dbobj.InsertTableData("Training")
dbobj.ExportFromDbToCsv()