from os import listdir, path
from sqlite3 import connect, OperationalError

# todo implement multiprocessing in some form

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


def getIndexes(paths):
	query = ""
	for Path in paths:
		query += '"' + Path + '",'
	query = query[0:len(query) - 1]
	responses = executeQuery("SELECT folder_path, folder_id FROM folders WHERE folder_path IN(" + query + ");")
	indexes = {}
	for response in responses:
		(Path, index, *rest) = response
		indexes[Path] = index
	return indexes


def updateDataBase(paths):
	print("Deleting data")
	cur.execute('DELETE FROM files;')
	cur.execute('DELETE FROM folders')
	con.commit()
	# createIndex()
	for Path in paths:
		cur.execute("INSERT INTO folders VALUES(NULL,\"" + Path + "\", NULL)")
		print("enumerating ", Path)
		addToDatabase(Path, getParentIndex(Path))
	con.commit()
	# print("creating indexes")
	# createIndex()
	print("vacuuming")
	vacuum()


def addToDatabase(Path, index):
	items = listdir(Path)
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
	parents = getIndexes(directories)
	dirData.clear()
	for directory in directories:
		try:
			addToDatabase(directory, parents[directory])
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


def insertFolderRecord(Path, parent):
	statement = "INSERT INTO folders VALUES(NULL, \"" + Path + "\",\"" + parent + "\");"
	cur.execute(statement)


def getParentIndex(Path):
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
