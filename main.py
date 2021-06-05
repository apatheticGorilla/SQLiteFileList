import databaseManager


db = databaseManager
# db.createDatabase()
# , "D:\\", "E:\\", "G:\\", "F:\\"
drives = ["C:\\"]
# db.updateDataBase(drives)
tuples = db.executeQuery("SELECT file_path FROM files WHERE extension IS NULL")
for item in tuples:
	(filePath, *rest) = item
	print(filePath)
