from os import listdir, path
from sqlite3 import connect, OperationalError


# noinspection PyMethodMayBeStatic
class databaseManager:
	con = connect('c:\\Temp\\files.db')
	cur = con.cursor()
	
	def __formatInQuery(self, clauses: list):
		query = ""
		for clause in clauses:
			query += '"' + str(clause) + '",'
		return query[0:len(query) - 1]
	
	def __compileFileData(self, basename: str, filepath: str, parent: (int, None)) -> tuple:
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
		# I know returning parameters I did nothing with is bad, but since tuples are immutable this was easier
		return basename, filepath, extension, size, parent
	
	def __scan(self, Path: str, parent: (int, None)):
		items = listdir(Path)
		fileData = []
		directories = []
		dirData = []
		for item in items:
			try:
				filepath = path.join(Path, item)
				if path.isfile(filepath):
					fileData.append(self.__compileFileData(item, filepath, parent))
				else:
					directories.append(filepath)
					dirData.append((item, filepath, parent))
			except FileNotFoundError:
				print("file not found: ", item)
		
		self.cur.executemany("INSERT INTO files (basename,file_path,extension,size,parent) VALUES(?,?,?,?,?)", fileData)
		self.cur.executemany("INSERT INTO folders (basename,folder_path, parent) VALUES (?,?,?)", dirData)
		parents = self.getIndexes(directories)
		dirData.clear()
		fileData.clear()
		
		for directory in directories:
			try:
				self.__scan(directory, parents[directory])
			except PermissionError:
				print("permission denied: ", directory)
			except FileNotFoundError:
				print("could not find:", directory)
			except OSError:
				print("OS error on:", directory)
			except KeyError:
				print('key error caught')
		
		directories.clear()
	
	def __getFolderIndex(self, Path: str) -> (str, None):
		try:
			result = self.cur.execute("SELECT folder_id FROM folders WHERE folder_path = '" + Path + "' LIMIT 1;")
			for r in result:
				(ID, *rest) = r
				if r is None:
					return None
				return str(ID)
		except OperationalError:
			print("failed to get index for", path)
			return None
	
	def __vacuum(self):
		self.cur.execute("VACUUM;")
		self.con.commit()
	
	def __createIndex(self):
		self.cur.executescript("""
		CREATE INDEX IF NOT EXISTS extension ON files(extension);
		CREATE INDEX IF NOT EXISTS size ON files(size);
		CREATE INDEX IF NOT EXISTS file_parent ON files(parent);
		CREATE INDEX IF NOT EXISTS folder_parent ON files(parent);
		""")
		self.con.commit()
	
	def __getChildDirectories(self, folders: list[any]):
		query = self.__formatInQuery(folders)
		parentsRaw = self.executeQuery("SELECT folder_id FROM folders WHERE parent IN(" + query + ");")
		children = []
		for p in parentsRaw:
			(ID, *drop) = p
			children.append(ID)
		if len(children) > 0:
			children.extend(self.__getChildDirectories(children))
		return children
	
	def createDatabase(self):
		print("recreating tables")
		self.cur.executescript("""
		DROP TABLE IF EXISTS files;
		DROP TABLE IF EXISTS folders;
		DROP INDEX IF EXISTS folder_path;
		CREATE TABLE IF NOT EXISTS files(
				file_id INTEGER PRIMARY KEY AUTOINCREMENT,
				basename TEXT,
				file_path TEXT,
				extension TEXT,
				size INT,
				parent INT,
				FOREIGN KEY (parent) REFERENCES folders (folder_id)
			);
			CREATE TABLE folders(
			folder_id INTEGER PRIMARY KEY AUTOINCREMENT,
			basename TEXT,
			folder_path TEXT,
			parent INT,
			FOREIGN KEY (parent) REFERENCES folders(folder_id)
		);
		CREATE UNIQUE INDEX folder_path ON folders(folder_path);
		VACUUM;
		""")
		self.con.commit()
	
	def getIndexes(self, paths: list[str]) -> dict[str, int]:
		query = self.__formatInQuery(paths)
		responses = self.executeQuery("SELECT folder_path, folder_id FROM folders WHERE folder_path IN(" + query + ");")
		indexes = {}
		for response in responses:
			(Path, index, *rest) = response
			indexes[Path] = index
		return indexes
	
	def updateDataBase(self, paths: list[str]):
		print("Deleting data")
		self.cur.executescript("""
			DELETE FROM files;
			DELETE FROM folders
		""")
		self.con.commit()
		for Path in paths:
			self.cur.execute("INSERT INTO folders VALUES(NULL,NULL,\"" + Path + "\", NULL)")
			print("enumerating ", Path)
			self.__scan(Path, self.__getFolderIndex(Path))
		self.con.commit()
		self.__createIndex()
		self.con.commit()
		print("vacuuming")
		self.__vacuum()
	
	def addFolder(self, Path: str):
		name = path.basename(Path)
		data = [(name, Path, self.__getFolderIndex(path.dirname(Path)))]
		self.cur.executemany("INSERT INTO folders (basename,folder_path, parent) VALUES (?,?,?)", data)
		self.__scan(Path, self.__getFolderIndex(Path))
		self.con.commit()
	
	def addFolders(self, paths: list[str]):
		for Path in paths:
			self.addFolder(Path)
	
	def executeQuery(self, query: str) -> list[tuple]:
		rows = []
		for row in self.cur.execute(query):
			rows.append(row)
		return rows
	
	def execute(self, script: str, commitOnCompletion: bool):
		self.cur.executescript(script)
		if commitOnCompletion:
			self.con.commit()
	
	def filesWithExtension(self, ext: (str, None)) -> list[tuple]:
		if ext is None:
			return self.executeQuery("SELECT * FROM files WHERE extension IS NULL")
		else:
			return self.executeQuery("SELECT * FROM files WHERE extension = '" + ext + "';")
	
	def removeFolder(self, folder: str, cleanup: bool):
		index = self.__getFolderIndex(folder)
		directories = self.__getChildDirectories([index])
		directories.append(index)
		query = self.__formatInQuery(directories)
		
		self.execute("DELETE FROM files WHERE parent IN(" + query + ")", True)
		self.execute("DELETE FROM folders WHERE folder_id IN(" + query + ")", True)
		if cleanup:
			self.__vacuum()
	
	def countItems(self, folder: str):
		index = self.__getFolderIndex(folder)
		total = 0
		c = self.executeQuery("SELECT COUNT(file_id) FROM files WHERE parent='" + index + "'")
		(count, *drop) = c[0]
		total += count
		children = self.__getChildDirectories([index])
		query = self.__formatInQuery(children)
		c = self.executeQuery("SELECT COUNT(file_id) FROM files WHERE parent IN(" + query + ")")
		(count, *drop) = c[0]
		total += count + len(children)
		return total
