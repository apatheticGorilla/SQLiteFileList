import databaseManager
import Timer


def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))


# first test(no index): 0:1:48.575974225997925
db = databaseManager

# db.executeQuery("DROP INDEX folder_path")
timer = Timer.Timer()
timer.startTime()
db.createDatabase()
# db.createIndex()
# , "D:\\", "E:\\", "G:\\", "F:\\"
drives = ["C:\\"]
db.updateDataBase(drives)
time_convert(timer.stopTime())
