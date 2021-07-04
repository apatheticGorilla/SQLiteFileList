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

# db.createDatabase()
# drives = ["C:\\", "D:\\", "G:\\", "F:\\", "Z:\\"]
# db.updateDataBase(drives)
util.pieChartByExtension(1000)

# print(db.executeQuery("SELECT COUNT(file_id) FROM files WHERE extension IS NULL"))
# simpleFile = open("C:\\temp\\files2.tfl", 'w', encoding="UTF-8")
# fileList = db.executeQuery("SELECT file_path FROM files WHERE size BETWEEN 5242880 AND 10485760 ORDER BY file_path ASC")
# for f in fileList:
# 	(Line, *drop) = f
# 	simpleFile.write(Line + "\n")
# simpleFile.close()
# db.execute("""DELETE FROM files WHERE file_path LIKE 'Z:\\%';
# 			DELETE FROM folders WHERE folder_path LIKE 'Z:\\%';""", True)
# db.vacuum()
