import databaseManager
import util

import Timer

def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))

db = databaseManager.databaseManager('C:\\Temp\\files.db', "C:\\Temp\\databaseManger.log")
# db = databaseManager.databaseManager('/home/hello/Documents/files.db', "/home/hello/Documents/databaseManger.log")
drives = ["C:\\", "D:\\", "F:\\", 'G:\\', 'E:\\']
db.createDatabase()
db.updateDataBase(drives, 0)
# db.updateDataBase([\], 0)

# util.writeTFLList("C:\\temp\\cabfiles.tfl", "extension = '.jar' ORDER BY size DESC")
# db.execute("""DELETE FROM files WHERE file_path LIKE 'Z:\\%';
# 			DELETE FROM folders WHERE folder_path LIKE 'Z:\\%';""", True)
