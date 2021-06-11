import databaseManager
import Timer


def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))
# 0:3:13.514529705047607


db = databaseManager

timer = Timer.Timer()
timer.startTime()
db.createDatabase()
# # , "D:\\", "E:\\", "G:\\", "F:\\"
drives = ["C:\\"]
# db.updateDataBase(drives)
db.addFolder("C:\\")
time_convert(timer.stopTime())
# db.executeQuery("CREATE INDEX file_path ON files(file_path)")
# db.executeQuery("CREATE INDEX folder_parent ON folders(parent)")
# db.executeQuery("CREATE INDEX file_parent ON files(parent)")
# db.executeQuery("CREATE INDEX size ON files(size)")

