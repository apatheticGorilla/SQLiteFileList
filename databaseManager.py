from os import listdir, path, mkdir
from sqlite3 import connect, OperationalError

# noinspection PyMethodMayBeStatic
from typing import Dict, List


class databaseManager:
	
	def __init__(self, Path):
		self.__con = connect(Path)
		self.__cur = self.__con.cursor()
	
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
		
		self.__cur.executemany("INSERT INTO files (basename,file_path,extension,size,parent) VALUES(?,?,?,?,?)",
							   fileData)
		self.__cur.executemany("INSERT INTO folders (basename,folder_path, parent) VALUES (?,?,?)", dirData)
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
		directories.clear()
	
	def __getFolderIndex(self, Path: str) -> (str, None):
		try:
			result = self.__cur.execute("SELECT folder_id FROM folders WHERE folder_path = '" + Path + "';")
			# assert len(result.fetchall()) <= 1
			for r in result:
				(ID, *rest) = r
				if r is None:
					return None
				return str(ID)
		except OperationalError:
			print("failed to get index for", Path)
			return None
	
	def __vacuum(self):
		self.__cur.execute("VACUUM;")
		self.__con.commit()
	
	def __createIndex(self):
		self.__cur.executescript("""
		CREATE INDEX IF NOT EXISTS extension ON files(extension);
		CREATE INDEX IF NOT EXISTS size ON files(size);
		CREATE INDEX IF NOT EXISTS file_parent ON files(parent);
		CREATE INDEX IF NOT EXISTS folder_parent ON folders(parent);
		""")
		self.__con.commit()
	
	def __getChildDirectories(self, folders: List[any], searchRecursively: bool):
		query = self.__formatInQuery(folders)
		parentsRaw = self.executeQuery("SELECT folder_id FROM folders WHERE parent IN(" + query + ");")
		children = []
		for p in parentsRaw:
			(ID, *drop) = p
			children.append(ID)
		if len(children) > 0 and searchRecursively:
			children.extend(self.__getChildDirectories(children, True))
		return children
	
	def createDatabase(self):
		print("recreating tables")
		self.__cur.executescript("""
		DROP TABLE IF EXISTS files;
		DROP TABLE IF EXISTS folders;
		DROP INDEX IF EXISTS folder_path;
		CREATE TABLE files(
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
		self.__con.commit()
	
	def updateDataBase(self, paths: List[str]):
		print("Deleting data")
		self.__cur.executescript("""
			DELETE FROM files;
			DELETE FROM folders
		""")
		self.__con.commit()
		for Path in paths:
			self.__cur.execute("INSERT INTO folders VALUES(NULL,NULL,\"" + Path + "\", NULL)")
			print("enumerating ", Path)
			self.__scan(Path, self.__getFolderIndex(Path))
		self.__con.commit()
		self.__createIndex()
		self.__con.commit()
		print("vacuuming")
		self.__vacuum()
	
	def getIndexes(self, paths: List[str]) -> Dict[str, int]:
		query = self.__formatInQuery(paths)
		responses = self.executeQuery("SELECT folder_path, folder_id FROM folders WHERE folder_path IN(" + query + ");")
		indexes = {}
		for response in responses:
			(Path, index, *rest) = response
			indexes[Path] = index
		return indexes
	
	def addFolder(self, Path: str):
		name = path.basename(Path)
		data = [(name, Path, self.__getFolderIndex(path.dirname(Path)))]
		self.__cur.executemany("INSERT INTO folders (basename,folder_path, parent) VALUES (?,?,?)", data)
		self.__scan(Path, self.__getFolderIndex(Path))
		self.__con.commit()
	
	def addFolders(self, paths: List[str]):
		for Path in paths:
			self.addFolder(Path)
	
	def execute(self, script: str, commitOnCompletion: bool):
		self.__cur.executescript(script)
		if commitOnCompletion:
			self.__con.commit()
	
	def executeQuery(self, query: str) -> List[tuple]:
		rows = []
		for row in self.__cur.execute(query):
			rows.append(row)
		# rows = self.__cur.execute(query)
		return rows
	
	def filesWithExtension(self, ext: (str, None)) -> List[tuple]:
		if ext is None:
			return self.executeQuery("SELECT * FROM files WHERE extension IS NULL")
		else:
			return self.executeQuery("SELECT * FROM files WHERE extension = '" + ext + "';")
	
	def removeFolder(self, folder: str, cleanup: bool):
		index = self.__getFolderIndex(folder)
		directories = self.__getChildDirectories([index], True)
		directories.append(index)
		query = self.__formatInQuery(directories)
		
		self.execute("DELETE FROM files WHERE parent IN(" + query + ")", True)
		self.execute("DELETE FROM folders WHERE folder_id IN(" + query + ")", True)
		if cleanup:
			self.__vacuum()
	
	# TODO diagnose memory issue
	def countItems(self, folder: str):
		index = self.__getFolderIndex(folder)
		total = 0
		c = self.executeQuery("SELECT COUNT(file_id) FROM files WHERE parent='" + index + "'")
		(count, *drop) = c[0]
		total += count
		# TODO revise. this causes high memory usage with lots of folders
		children = self.__getChildDirectories([index], True)
		query = self.__formatInQuery(children)
		c = self.executeQuery("SELECT COUNT(file_id) FROM files WHERE parent IN(" + query + ")")
		(count, *drop) = c[0]
		total += count + len(children)
		children.clear()
		return total
	
	def recreateFolderStructure(self, outFolder: str, refFolder: str):
		cleanOutput = refFolder.replace(":", "")
		try:
			mkdir(path.join(outFolder, cleanOutput))
		except FileNotFoundError:
			pass
		index = self.__getFolderIndex(refFolder)
		children = self.__formatInQuery(self.__getChildDirectories([index], False))
		childDirs = self.executeQuery("SELECT folder_path FROM folders WHERE folder_id IN(" + children + ")")
		for child in childDirs:
			(direc, *drop) = child
			self.recreateFolderStructure(outFolder, direc)
			
	def recreateFileStructure(self, outFolder, refFolder):
		cleanOutput = refFolder.replace(":", "")
		outPath = path.join(outFolder, cleanOutput)
		try:
			mkdir(outPath)
		except FileNotFoundError:
			pass
		index = self.__getFolderIndex(refFolder)
		if index is not None:
			files = self.executeQuery("SELECT basename FROM files WHERE parent = '" + index + "';")
			for f in files:
				(file, *drop) = f
				try:
					open(path.join(outPath,file),'x')
				except FileNotFoundError:
					continue
		children = self.__formatInQuery(self.__getChildDirectories([index], False))
		childDirs = self.executeQuery("SELECT folder_path FROM folders WHERE folder_id IN(" + children + ")")
		for child in childDirs:
			(direc, *drop) = child
			self.recreateFileStructure(outFolder, direc)
			
	def testFunction(self):
		pass
	
	def vacuum(self):
		self.__vacuum()
