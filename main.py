import sys
import databaseManager
import gc

db = databaseManager
db.createDatabase()
# , "D:\\", "E:\\", "G:\\", "F:\\"
drives = ["C:\\", "D:\\", "E:\\", "G:\\", "F:\\"]
db.updateDataBase(drives)
# db.index()
# db.vacuum()

