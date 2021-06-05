import os
import sqlite3

con = sqlite3.connect('test.db')
cur = con.cursor()


def addToDatabase(path):
	items = os.listdir(path)
	# print(items)
	try:
		for item in items:
			filePath = os.path.join(path, item)
			if os.path.isfile(filePath):
				insertRecord(filePath)
			else:
				try:
					addToDatabase(filePath)
				except PermissionError:
					continue
	
	except FileNotFoundError:
		print("file not found: ", item)


def updateDataBase(paths):
	print("Deleting data")
	cur.execute('DELETE FROM files;')
	con.commit()
	for path in paths:
		print("enumerating ", path)
		addToDatabase(path)
	con.commit()
	print("vacuuming")
	vacuum()
	print("done")


def insertRecord(filePath):
	isNull = False
	try:
		name = os.path.basename(filePath)
		extension = name[name.rindex('.'):]
	except ValueError:
		isNull = True
	try:
		size = os.path.getsize(filePath)
		if isNull:
			statement = "INSERT INTO files VALUES(NULL,\"" + filePath + "\", NULL,'" + str(size) + "')"
			cur.execute(statement)
		else:
			cur.execute(
				"INSERT INTO files VALUES(NULL,\"" + filePath + "\",'" + extension + "','" + str(
					size) + "')")
	except sqlite3.OperationalError:
		print('failed to add file: ', filePath)
	except FileNotFoundError:
		print('file decided it didn\'t exist: ', filePath)


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
