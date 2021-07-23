from datetime import datetime
class logger:
    """
    created by:monoit Saha
    description:this class is used for writting the logs in the log files
    version:1.0
    revision :none
    """
    def __init__(self):
        pass


    def writelog(self,fileobject,messages):
        """
        created by:Monojit Saha
        description:this method  is used to write a message along with date and time in a text file
        :param fileobject:
        :param messages:
        version:1.0
        revision:none
        :return:
        """

        self.date=datetime.now().date()
        self.current_time=datetime.now().time().strftime("%HH-%MM-%SS")
        self.message=str(self.date)+"\t"+str(self.current_time)+"\t\t"+messages+"\n"
        fileobject.write(self.message)