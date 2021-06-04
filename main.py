import sqlite3
import os


def getFileList(path):
	files = []
	try:
		items = os.listdir(path)
	except FileNotFoundError:
		# might be mixing folders into files
		files.append(path)
		return files
	
	for item in items:
		if os.path.isfile(os.path.join(path, item)):
			files.append(os.path.join(path, item))
		else:
			try:
				for file in getFileList(os.path.join(path, item)):
					files.append(file)
			except PermissionError:
				continue
	return files


def updateDataBase(path):
	con.execute('DELETE FROM files;')
	for filePath in getFileList(path):
		isFile = True
		isNull = False
		try:
			extension = filePath[filePath.rindex('.'):]
		except ValueError:
			isFile = os.path.isfile(filePath)
			extension = "NULL"
			isNull = True
		if isFile:
			if isNull:
				cur.execute("INSERT INTO files VALUES(NULL,'" + filePath + "'," + extension + ")")
			else:
				try:
					cur.execute("INSERT INTO files VALUES(NULL,'" + filePath + "','" + extension + "')")
				except sqlite3.OperationalError:
					print('failed to add file: ', filePath)
		else:
			print("folder")
	con.commit()


con = sqlite3.connect('test.db')
cur = con.cursor()
updateDataBase("C:\\")

for row in cur.execute("SELECT fullPath FROM files WHERE extension='NULL';"):
	print(row)
