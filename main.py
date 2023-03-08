import os.path as path

import databaseManager
import util

import Timer


def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))


# db = databaseManager.databaseManager('C:\\Temp\\files.db', "C:\\Temp\\databaseManager.log")
home = path.expanduser("~")
dbFolder = path.join(home, 'Documents/SQLiteDatabase')
db = databaseManager.databaseManager(path.join(dbFolder, 'files.db'), path.join(dbFolder, 'logs/databaseManager.log'))
drives = ["C:\\", "D:\\", "F:\\", 'G:\\', 'E:\\']
# db.createDatabase()
timer = Timer.Timer()
timer.startTime()
# db.updateDataBase(drives, 0)
# db.updateDatabase(['/'], 0)
db.recreateFileStructure(path.join(home, 'Documents/filestructure'),'/')
"""
db.createDatabase()
db.updateDatabase(['/media'], 0)
db.reportDbStats()
db.resetDbStats()
db.addFolder('/home', 0)
db.addFolders(['/cdrom', '/dev', '/etc'],0)
db.removeFolder('/home', False)
db.recreateFolderStructure(path.join(home, 'Documents/folderstructure'),'/dev')
db.recreateFileStructure(path.join(home, 'Documents/filestructure'),'/dev')
db.execute("INSERT INTO folders (basename,folder_path, parent) VALUES ('fake','fake',-1)", True)
print(db.executeQuery("SELECT * FROM folders WHERE basename='fake'"))
print(db.countItems('/media'))
print(db.filesWithExtension('.cpp'))
print(db.AvgFileSize('/media'))
print(db.MedianFileSize('/media'))
db.testFunction()
db.vacuum()
"""
print(time_convert(timer.stopTime()))

# util.writeTFLList("C:\\temp\\cabfiles.tfl", "extension = '.jar' ORDER BY size DESC")
# db.execute("""DELETE FROM files WHERE file_path LIKE 'Z:\\%';
# 			DELETE FROM folders WHERE folder_path LIKE 'Z:\\%';""", True)
