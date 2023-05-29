import os.path as path
import platform
import databaseManager
import util
import Timer


def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))


db = None
timer = Timer.Timer()
timer.startTime()
if platform.system() == 'Linux':
	home = path.expanduser("~")
	dbFolder = path.join(home, 'Documents/SQLiteDatabase')
	db = databaseManager.databaseManager(path.join(dbFolder, 'files.db'), path.join(dbFolder, 'logs/DatabaseManager.log'))
	db.updateDatabase(['/'], 0)
elif platform.system() == 'Windows':
	# drives = ["C:\\", "D:\\", "F:\\", 'G:\\', 'E:\\']
	# yes, this is bad, but I don't know what will happen if I attempt to use win32api on linux
	import win32api

	drives = win32api.GetLogicalDriveStrings()
	drives = drives.split('\000')[:-1]
	db = databaseManager.databaseManager('C:\\Temp\\files.db', "C:\\Temp\\DatabaseManager.log")
	db.updateDatabase(drives, 0)
else:
	print('Unknown OS: ' + platform.system())
time_convert(timer.stopTime())
# db.createDatabase()
