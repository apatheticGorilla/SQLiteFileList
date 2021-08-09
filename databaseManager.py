import os.path
from os import listdir, path
from sqlite3 import connect, OperationalError

con = connect('c:\\Temp\\files.db')
cur = con.cursor()


def createDatabase():
	print("recreating tables")
	cur.executescript("""
	DROP TABLE IF EXISTS files;
	DROP TABLE IF EXISTS folders;
	DROP INDEX IF EXISTS folder_path;
	CREATE TABLE IF NOT EXISTS files(
			file_id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_path TEXT,
			extension TEXT,
			size INT,
			parent INT,
			FOREIGN KEY (parent) REFERENCES folders (folder_id)
		);
		CREATE TABLE folders(
		folder_id INTEGER PRIMARY KEY AUTOINCREMENT,
		folder_path TEXT,
		parent INT,
		FOREIGN KEY (parent) REFERENCES folders(folder_id)
	);
	CREATE UNIQUE INDEX folder_path ON folders(folder_path);
	CREATE INDEX size ON files(size);
	VACUUM;
	""")
	con.commit()


def createIndex():
	cur.executescript("""
	CREATE INDEX IF NOT EXISTS extension ON files(extension);
	""")
	con.commit()


def getIndexes(paths: list[str]) -> dict[str, int]:
	query = formatInQuery(paths)
	responses = executeQuery("SELECT folder_path, folder_id FROM folders WHERE folder_path IN(" + query + ");")
	indexes = {}
	for response in responses:
		(Path, index, *rest) = response
		indexes[Path] = index
	return indexes


def updateDataBase(paths: list[str]):
	print("Deleting data")
	cur.executescript("""
		DELETE FROM files;
		DELETE FROM folders
	""")
	con.commit()
	for Path in paths:
		cur.execute("INSERT INTO folders VALUES(NULL,\"" + Path + "\", NULL)")
		print("enumerating ", Path)
		scan(Path, getFolderIndex(Path))
	con.commit()
	createIndex()
	con.commit()
	print("vacuuming")
	vacuum()


def formatInQuery(clauses: list):
	query = ""
	for clause in clauses:
		query += '"' + str(clause) + '",'
	return query[0:len(query) - 1]


def scan(Path: str, parent: (int, None)):
	items = listdir(Path)
	fileData = []
	directories = []
	dirData = []
	for item in items:
		try:
			filepath = path.join(Path, item)
			if path.isfile(filepath):
				fileData.append(compileFileData(filepath, parent))
			else:
				directories.append(filepath)
				dirData.append((filepath, parent))
		except FileNotFoundError:
			print("file not found: ", item)
	
	writeFileData(fileData)
	fileData.clear()
	writeFolderData(dirData)
	parents = getIndexes(directories)
	dirData.clear()
	
	for directory in directories:
		try:
			scan(directory, parents[directory])
		except PermissionError:
			print("permission denied: ", directory)
		except FileNotFoundError:
			
			print("could not find:", directory)
		except OSError:
			print("OS error on:", directory)
	directories.clear()


def compileFileData(filepath: str, parent: (int, None)) -> tuple:
	size = 0
	try:
		name = path.basename(filepath)
		extension = name[name.rindex('.'):]
	except ValueError:
		extension = None
	try:
		size = path.getsize(filepath)
	except FileNotFoundError:
		print("file not found; ", filepath)
	return filepath, extension, size, parent


def writeFileData(data: list[tuple]):
	cur.executemany("INSERT INTO files (file_path,extension,size,parent) VALUES(?,?,?,?)", data)


def writeFolderData(data: list[tuple]):
	cur.executemany("INSERT INTO folders (folder_path, parent) VALUES (?,?)", data)


def addFolder(Path: str):
	writeFolderData([(Path, getFolderIndex(os.path.dirname(Path)))])
	scan(Path, getFolderIndex(Path))
	con.commit()


def addFolders(paths: list[str]):
	for Path in paths:
		addFolder(Path)


def getFolderIndex(Path: str) -> (str, None):
	try:
		result = cur.execute("SELECT folder_id FROM folders WHERE folder_path = '" + Path + "' LIMIT 1;")
		for r in result:
			(ID, *rest) = r
			if r is None:
				return None
			return str(ID)
	except OperationalError:
		print("failed to get index for", path)
		return None


def vacuum():
	cur.execute("VACUUM;")
	con.commit()


def executeQuery(query: str) -> list[tuple]:
	rows = []
	for row in cur.execute(query):
		rows.append(row)
	return rows


def execute(script: str, commitOnCompletion: bool):
	cur.executescript(script)
	if commitOnCompletion:
		con.commit()


def filesWithExtension(ext: (str, None)) -> list[tuple]:
	if ext is None:
		return executeQuery("SELECT * FROM files WHERE extension IS NULL")
	else:
		return executeQuery("SELECT * FROM files WHERE extension = '" + ext + "';")


def removeFolder(folder: str, cleanup: bool):
	index = getFolderIndex(folder)
	directories = getChildDirectories([index])
	directories.append(index)
	query = formatInQuery(directories)
	
	execute("DELETE FROM files WHERE parent IN(" + query + ")", True)
	execute("DELETE FROM folders WHERE folder_id IN(" + query + ")", True)
	if cleanup:
		vacuum()


def getChildDirectories(folders: list[any]):
	query = formatInQuery(folders)
	parentsRaw = executeQuery("SELECT folder_id FROM folders WHERE parent IN(" + query + ");")
	children = []
	for p in parentsRaw:
		(ID, *drop) = p
		children.append(ID)
	if len(children) > 0:
		children.extend(getChildDirectories(children))
	return children
