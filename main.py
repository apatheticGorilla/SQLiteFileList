import databaseManager
import util


# import Timer


def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))


db = databaseManager.databaseManager('C:\\Temp\\files.db')

drives = ["C:\\", "D:\\", "F:\\", 'E:\\', 'Z:\\','G:\\']

# rebuildDatabase(drives)
# db.removeFolder("E:\\", True)
# db.addFolder("E:\\")
# db.createIndex()
print(db.countItems("E:\\New folder"))

# util.writeTFLList("C:\\temp\\files2.tfl", " = '.png' ORDER BY size DESC")
# db.execute("""DELETE FROM files WHERE file_path LIKE 'Z:\\%';
# 			DELETE FROM folders WHERE folder_path LIKE 'Z:\\%';""", True)

