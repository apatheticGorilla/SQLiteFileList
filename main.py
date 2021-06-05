import databaseManager


db = databaseManager
# db.createDatabase()
drives = ["C:\\", "D:\\", "E:\\", "G:\\", "F:\\"]
db.updateDataBase(drives)

print(db.executeQuery("SELECT sum(size), extension FROM files GROUP BY(extension) ORDER BY sum(size) DESC;"))
