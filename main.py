import databaseManager
import util


# import Timer


def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))


db = databaseManager


def rebuildDatabase(folders):
	db.createDatabase()
	db.updateDataBase(folders)


# print(db.executeQuery("SELECT COUNT(file_id) FROM files WHERE extension IS NULL"))
util.writeTFLList("C:\\temp\\files2.tfl", "extension = '.mp4'")
# db.execute("""DELETE FROM files WHERE file_path LIKE 'Z:\\%';
# 			DELETE FROM folders WHERE folder_path LIKE 'Z:\\%';""", True)
# db.vacuum()
