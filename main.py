import databaseManager
# import util


# import Timer


def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))


db = databaseManager.databaseManager('C:\\Temp\\files.db')

drives = ["C:\\", "D:\\", "F:\\", 'G:\\']
# db.createDatabase()
# db.recreateFolderStructure('E:\\', 'C:\\')
# db.updateDataBase(drives)
# db.execute('CREATE INDEX folder_parents ON folders(parent)', commitOnCompletion=True)
total_size = db.countItems('D:\\fuck')
print(total_size)
# db.vacuum()

# util.writeTFLList("C:\\temp\\cabfiles.tfl", "extension = '.jar' ORDER BY size DESC")
# db.execute("""DELETE FROM files WHERE file_path LIKE 'Z:\\%';
# 			DELETE FROM folders WHERE folder_path LIKE 'Z:\\%';""", True)
