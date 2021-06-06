import databaseManager
import Timer


def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))


# c drive no index 0:30:19.857494592666626
db = databaseManager

# db.executeQuery("DROP INDEX folder_path")
timer = Timer.Timer()
timer.startTime()
db.createDatabase()
# db.createIndex()
# , "D:\\", "E:\\", "G:\\", "F:\\"
drives = ["C:\\", "D:\\", "E:\\", "G:\\", "F:\\", "Z:\\"]
db.updateDataBase(drives)
time_convert(timer.stopTime())
