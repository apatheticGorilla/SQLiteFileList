import databaseManager

# TODO add table for folders

db = databaseManager
# db.createDatabase()
drives = ["C:\\", "D:\\", "E:\\", "G:\\", "F:\\", "Z:\\"]
db.updateDataBase(drives)
print(db.executeQuery("SELECT sum(size), extension FROM files GROUP BY(extension) LIMIT 100"))
