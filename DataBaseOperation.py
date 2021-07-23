import sqlite3
from log import logger
import pandas
import os
import csv
import shutil
import csv


class DataBaseOperation:
    def __init__(self):
        self.good_data_path = "training_good_&_bad_files_directory/good_raw_files"
        self.bad_file_path = "training_good_&_bad_files_directory/bad_raw_files"
        self.data_base_folder = "Training Database"
        self.createlog = logger()

    def ConnectToDB(self, databasename):
        file = databasename + ".db"
        try:
            conn = sqlite3.connect(self.data_base_folder+"/"+databasename + ".db")
            f = open("logs/Train/DatabaseConnectionLog.txt", "a+")
            self.createlog.writelog(f, "%s created " % file)
            f.close()
            return conn
        #except Exception as e:
           # f = open("logs/Train/DatabaseConnectionLog.txt", "a+")
            #self.createlog.writelog(f, "Error !!! %s" % str(e))
            #f.close()
            #raise e
        except ConnectionError as c:
            f = open("logs/Train/DatabaseConnectionLog.txt", "a+")
            self.createlog.writelog(f, "Connection Error !!! %s" % str(c))
            f.close()
        except sqlite3.Error as s:
            f = open("logs/Train/DatabaseConnectionLog.txt", "a+")
            self.createlog.writelog(f, "Sqlite3 Error !!! %s" % str(s))
            f.close()
            raise sqlite3.Error

    def createtable(self, dbasename, columnnames):
        conn = self.ConnectToDB(dbasename)
        c = conn.cursor()
        # c.execute("CREATE TABLE GOOD_RAW_DATA (wafer varchar)")
        # c.execute("CREATE TABLE GOOD_RAW (wafer varchar)")
        tablecolumn = list()
        for key, value in columnnames.items():
            k = key.replace(' ', '')
            k = k.replace('-', '')
            tablecolumn.append(k + " " + value)
        tablecolumn = str(tablecolumn).replace("\'", '').replace('[', '').replace(']', '')
        # print("CREATE TABLE GOOD_RAW_DATA ({columnname});".format(columnname=tablecolumn))
        numberoftales = c.execute("SELECT COUNT(NAME) FROM Sqlite_master where type='table' and name='GOOD_RAW_DATA'")
        no_of_tables = c.fetchall()[0][0]
        try:
            if (no_of_tables == 1):
                file = open("logs/Train/CreateTable.txt", "a+")
                self.createlog.writelog(file, "Table created Successfully!!!")
                file.close()
            else:
                c.execute("CREATE TABLE GOOD_RAW_DATA ({columnname});".format(columnname=tablecolumn))
                c.close()
                conn.close()
                f = open("logs/Train/CreateTable.txt", "a+")
                self.createlog.writelog(f, "Table created successfully!!!")
                f.close()
        except Exception as E:
            f = open("logs/Train/CreateTable.txt", "a+")
            self.createlog.writelog(f, "%s!!!" % str(E))
            f.close()

    def InsertTableData(self, Database):
        conn = self.ConnectToDB(Database)
        c = conn.cursor()
        try:
            count = 0
            for file in os.listdir("training_good_&_bad_files_directory/good_raw_files"):
                count += 1
                print("processing " + file)
                try:
                    csv_file = pandas.read_csv("training_good_&_bad_files_directory/good_raw_files/" + file)
                    print("rows=" + str(csv_file.shape[0]))
                    for i in range(0, csv_file.shape[0]):
                        print("inserting " + str(i) + " row")
                        row = ''
                        row = str(list(csv_file.loc[i])).replace('\'Null\'', 'Null').replace('[', '')
                        row = row.replace(']', '')
                        # print(row)
                        c.execute("INSERT INTO GOOD_RAW_DATA VALUES ({val})".format(val=row))
                    fileobj = open("logs/Train/InsertDataInTable.txt", "a+")
                    self.createlog.writelog(fileobj,
                                            "values inserted from {count} . {file}".format(count=count, file=file))
                    fileobj.close()
                except Exception as Exc:
                    shutil.move(self.good_data_path + "/" + file, self.bad_file_path + "/" + file)
                    fileobj = open("logs/Train/InsertDataInTable.txt", "a+")
                    self.createlog.writelog(fileobj, "%s moved to Bad_raw_files due to %s" % (file, Exc))
                    fileobj.close()
                    raise Exc
            conn.commit()
        except Exception as E:
            conn.rollback()
            fileobj = open("logs/Train/InsertDataInTable.txt", "a+")
            self.createlog.writelog(fileobj, "{e}".format(e=E))
            fileobj.close()

    def ExportFromDbToCsv(self):
        self.file_from_db_location = "trainingfilefromdb/"
        self.csv_name = "input.csv"
        try:
            conn = self.ConnectToDB("Training")
            c = conn.cursor()
            sql_select = 'SELECT * FROM GOOD_RAW_DATA'
            c.execute(sql_select)
            result = c.fetchall()
            column_names = [i[0] for i in list(c.description)]
            file=open(file=self.file_from_db_location+self.csv_name,mode='w',newline='')
            csv_writer = csv.writer(file,delimiter=',',lineterminator='\r\n', escapechar='\\')
            csv_writer.writerow(column_names) # writting header
            csv_writer.writerows(result)# writting header
            conn.close()
            log_file = open('logs/Train/ExportToCsv.txt', 'a+')
            self.createlog.writelog(log_file,"%s Exported Successfully !!!" % self.csv_name)
            log_file.close()
        except Exception as E:
            log_file=open('logs/Train/ExportToCsv.txt','a+')
            self.createlog.writelog(log_file,"Error !!! %s" % str(E))
            log_file.close()
        except ConnectionError as C:
            log_file = open('logs/Train/ExportToCsv.txt', 'a+')
            self.createlog.writelog(log_file, "connection Error Error !!! %s" % str(C))
            log_file.close()




