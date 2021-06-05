import os
import sqlite3

con = sqlite3.connect('test.db')
cur = con.cursor()


def addToDatabase(path):
	items = os.listdir(path)
	# print(items)
	for item in items:
		try:
			filepath = os.path.join(path, item)
			if os.path.isfile(filepath):
				insertFileRecord(filepath)
			else:
				try:
					insertFolderRecord(filepath)
					addToDatabase(filepath)
				except PermissionError:
					print("permission error")
					continue
		except FileNotFoundError:
			print("file not found: ", item)


def updateDataBase(paths):
	print("Deleting data")
	cur.execute('DELETE FROM files;')
	cur.execute('DELETE FROM folders')
	con.commit()
	for path in paths:
		print("enumerating ", path)
		addToDatabase(path)
	print("committing")
	con.commit()
	print("vacuuming")
	vacuum()
	print("done")


def getParentIndex(path):
	# print(path)
	parentPath = os.path.dirname(path)
	
	# response = executeQuery("SELECT folder_id FROM folders WHERE folder_path = \"" + parentPath + "\";")
	try:
		result = cur.execute("SELECT folder_id FROM folders WHERE folder_path = '" + parentPath + "' LIMIT 1;")
		for r in result:
			(index, *rest) = r
			if r is None:
				return None
			return str(index)
	except sqlite3.OperationalError:
		return None


def insertFileRecord(filePath):
	isnull = False
	parent = os.path.dirname(filePath)
	try:
		name = os.path.basename(filePath)
		extension = name[name.rindex('.'):]
	except ValueError:
		isnull = True
	try:
		size = os.path.getsize(filePath)
		if isnull:
			cur.execute(
				"INSERT INTO files VALUES(NULL,\"" + filePath + "\", NULL,'" + str(size) + "',\"" + parent + "\")")
		else:
			cur.execute(
				"INSERT INTO files VALUES(NULL,\"" + filePath + "\",'" + extension + "','" + str(
					size) + "',\"" + parent + "\")")
	except FileNotFoundError:
		print('file decided it didn\'t exist: ', filePath)


def insertFolderRecord(path):
	statement = "INSERT INTO folders VALUES(NULL, \"" + path + "\",\"" + os.path.join(path) + "\");"
	cur.execute(statement)


# parent = getParentIndex(path)
# if parent is None:
# 	cur.execute("INSERT INTO folders VALUES(NULL, \"" + path + "\",NULL);")
# else:
# 	cur.execute("INSERT INTO folders VALUES(NULL, \"" + path + "\", '" + parent + "');")


def vacuum():
	cur.execute("VACUUM;")
	con.commit()


def createDatabase():
	print("recreating tables")
	cur.execute("DROP TABLE IF EXISTS files;")
	cur.execute("DROP TABLE IF EXISTS folders;")
	con.commit()
	cur.execute('''
		CREATE TABLE IF NOT EXISTS files(
			file_id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_path TEXT,
			extension TEXT,
			size INT,
			parent TEXT,
			FOREIGN KEY (parent) REFERENCES folders (folder_path)
		);
	''')
	cur.execute('''
	CREATE TABLE folders(
		folder_id INTEGER PRIMARY KEY AUTOINCREMENT,
		folder_path TEXT,
		parent TEXT,
		FOREIGN KEY (parent) REFERENCES folders(folder_path)
	);
	''')
	con.commit()
	vacuum()


def executeQuery(query):
	rows = []
	for row in cur.execute(query):
		rows.append(row)
	return rows
