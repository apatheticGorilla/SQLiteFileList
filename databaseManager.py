import os
import sqlite3

con = sqlite3.connect('test.db')
cur = con.cursor()


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


def addToDatabase(path):
	for filePath in getFileList(path):
		isFile = True
		isNull = False
		try:
			extension = filePath[filePath.rindex('.'):]
		except ValueError:
			isFile = os.path.isfile(filePath)
			extension = "NULL"
			isNull = True
		try:
			if isFile:
				if isNull:
					statement = "INSERT INTO files VALUES(NULL,'" + filePath + "', NULL)"
					cur.execute(statement)
				else:
					# try\except should encapsulate all execute statements
					cur.execute("INSERT INTO files VALUES(NULL,'" + filePath + "','" + extension + "')")
		except sqlite3.OperationalError:
			print('failed to add file: ', filePath)
	con.commit()


def createDatabase():
	cur.execute('''
		CREATE TABLE IF NOT EXISTS files(
			file_id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_path TEXT,
			extension TEXT,
			size INT
		);
	''')
	con.commit()
	
	
def executeQuery(query):
	return cur.execute(query)


class DatabaseManager:
	pass
