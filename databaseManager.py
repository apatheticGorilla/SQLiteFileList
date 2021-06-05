import os
import re
import sqlite3

con = sqlite3.connect('test.db')
cur = con.cursor()


def getFileList(path):
	files = []
	try:
		items = os.listdir(path)
	except FileNotFoundError:
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


def updateDataBase(paths):
	cur.execute('DELETE FROM files;')
	con.commit()
	for path in paths:
		addToDatabase(path)
	con.commit()
	vacuum()


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
				size = os.path.getsize(filePath)
				if isNull:
					statement = "INSERT INTO files VALUES(NULL,\"" + re.escape(filePath) + "\", NULL,'" + str(size) + "')"
					cur.execute(statement)
				else:
					# try\except should encapsulate all execute statements
					cur.execute(
						"INSERT INTO files VALUES(NULL,\"" + re.escape(filePath) + "\",'" + extension + "','" + str(size) + "')")
		except sqlite3.OperationalError:
			print('failed to add file: ', re.escape(filePath))
		except FileNotFoundError:
			print('file decided it didn\'t exist: ', filePath)
	con.commit()


def vacuum():
	cur.execute("VACUUM;")
	con.commit()


def createDatabase():
	cur.execute("DROP TABLE files;")
	con.commit()
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
	rows = []
	for row in cur.execute(query):
		rows.append(row)
	return rows
