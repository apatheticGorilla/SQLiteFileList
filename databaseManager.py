from os import listdir, path
from sqlite3 import connect, OperationalError

# TODO see if indexing before inserting data improves performance
# TODO re-organize functions
con = connect('test.db')
cur = con.cursor()


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
			parent INT,
			FOREIGN KEY (parent) REFERENCES folders (folder_id)
		);
	''')
	cur.execute('''
	CREATE TABLE folders(
		folder_id INTEGER PRIMARY KEY AUTOINCREMENT,
		folder_path TEXT,
		parent INT,
		FOREIGN KEY (parent) REFERENCES folders(folder_id)
	);
	''')
	con.commit()
	vacuum()


def createIndex():
	cur.execute("CREATE INDEX folder_path ON folders(folder_id,folder_path)")
	con.commit()


def updateDataBase(paths):
	print("Deleting data")
	cur.execute('DELETE FROM files;')
	cur.execute('DELETE FROM folders')
	con.commit()
	# createIndex()
	for Path in paths:
		cur.execute("INSERT INTO folders VALUES(NULL,\"" + Path + "\", NULL)")
		print("enumerating ", Path)
		addToDatabase(Path)
	con.commit()
	print("creating indexes")
	createIndex()
	print("vacuuming")
	vacuum()


def addToDatabase(Path):
	items = listdir(Path)
	index = getParentIndex(Path)
	fileData = []
	directories = []
	dirData = []
	for item in items:
		try:
			filepath = path.join(Path, item)
			if path.isfile(filepath):
				fileData.append(compileFileData(filepath, index))
			else:
				directories.append(filepath)
				dirData.append((filepath, index))
		except FileNotFoundError:
			print("file not found: ", item)
	writeFileData(fileData)
	fileData.clear()
	writeFolderData(dirData)
	dirData.clear()
	for directory in directories:
		try:
			addToDatabase(directory)
		except PermissionError:
			print("permission denied: ", directory)
		except FileNotFoundError:
			print("could not find:", directory)
	directories.clear()


def compileFileData(filepath, parent):
	isnull = False
	size = None
	try:
		name = path.basename(filepath)
		extension = name[name.rindex('.'):]
	except ValueError:
		isnull = True
	if isnull:
		extension = None
	try:
		size = path.getsize(filepath)
	except FileNotFoundError:
		print("file not found; ", filepath)
	return filepath, extension, size, parent


def writeFileData(data):
	cur.executemany("INSERT INTO files (file_path,extension,size,parent) VALUES(?,?,?,?)", data)
	

def writeFolderData(data):
	cur.executemany("INSERT INTO folders (folder_path, parent) VALUES (?,?)", data)
	
	
# def insertFileRecord(filePath, parent):
# 	global extension
# 	isnull = False
# 	# parent = os.path.dirname(filePath)
# 	try:
# 		name = path.basename(filePath)
# 		extension = name[name.rindex('.'):]
# 	except ValueError:
# 		isnull = True
# 	try:
# 		size = path.getsize(filePath)
# 		if isnull:
# 			if parent is None:
# 				cur.execute(
# 					"INSERT INTO files VALUES(NULL,\"" + filePath + "\", NULL,'" + str(size) + "',NULL);")
# 			else:
# 				cur.execute(
# 					"INSERT INTO files VALUES(NULL,\"" + filePath + "\", NULL,'" + str(size) + "',\"" + parent + "\")")
# 		else:
# 			if parent is None:
# 				cur.execute(
# 					"INSERT INTO files VALUES(NULL,\"" + filePath + "\",'" + extension + "','" + str(
# 						size) + "',NULL);")
# 			else:
# 				cur.execute(
# 					"INSERT INTO files VALUES(NULL,\"" + filePath + "\",'" + extension + "','" + str(
# 						size) + "','" + parent + "')")
# 	except FileNotFoundError:
# 		print('file decided it didn\'t exist: ', filePath)


def insertFolderRecord(Path, parent):
	statement = "INSERT INTO folders VALUES(NULL, \"" + Path + "\",\"" + parent + "\");"
	cur.execute(statement)


def getParentIndex(Path):
	# print(path)
	
	# response = executeQuery("SELECT folder_id FROM folders WHERE folder_path = \"" + parentPath + "\";")
	try:
		result = cur.execute("SELECT folder_id FROM folders WHERE folder_path = '" + Path + "' LIMIT 1;")
		for r in result:
			(ID, *rest) = r
			if r is None:
				return None
			return str(ID)
	except OperationalError:
		return None


def vacuum():
	cur.execute("VACUUM;")
	con.commit()


def executeQuery(query):
	rows = []
	for row in cur.execute(query):
		rows.append(row)
	return rows
