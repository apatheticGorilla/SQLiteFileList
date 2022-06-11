from os import listdir, path, mkdir
from sqlite3 import connect, OperationalError

# noinspection PyMethodMayBeStatic
from typing import Dict, List


class databaseManager:
	
	def __init__(self, Path):
		dbExists = path.exists(Path)
		self.__con = connect(Path)
		self.__cur = self.__con.cursor()
		self.__queryCount = 0
		self.__updateCount = 0
		if not dbExists:
			print('no file was found, creating tables')
			self.createDatabase()
	
	def __formatInQuery(self, clauses: list):
		query = ""
		for clause in clauses:
			query += '"' + str(clause) + '",'
		return query[0:len(query) - 1]
	
	# get file info for the database
	def __getFileInfo(self, basename: str, filepath: str, parent: (int, None)) -> tuple:
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
	
	# used to recursively scan a selected folder
	def __scan(self, Path: str, parent: (int, None)):
		fileData = []
		directories = []
		dirData = []
		
		items = listdir(Path)
		for item in items:
			try:
				filepath = path.join(Path, item)
				# check if the path is a file or folder and put into the appropriate list
				if path.isfile(filepath):
					fileData.append(self.__getFileInfo(item, filepath, parent))
				else:
					directories.append(filepath)
					dirData.append((item, filepath, parent))
			except FileNotFoundError:
				print("file not found: ", item)
		
		# add items to database
		self.__updateCount += 1
		self.__cur.executemany("INSERT INTO files (basename,file_path,extension,size,parent) VALUES(?,?,?,?,?)",
							   fileData)
		self.__updateCount += 1
		self.__cur.executemany("INSERT INTO folders (basename,folder_path, parent) VALUES (?,?,?)", dirData)
		
		parents = self.getIndexes(directories)
		# rinse and repeat
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
			result = self.__cur.execute("SELECT folder_id FROM folders WHERE folder_path =:Path",
										{"Path": Path}).fetchall()
			self.__queryCount += 1
			# folder_path is unique, so it would be incredible if this failed before a database insertion
			assert len(result) <= 1
			if len(result) == 0:
				return None
			(ID, *rest) = result[0]
			return str(ID)
		except OperationalError:
			print("failed to get index for", Path)
			return None
	
	def __vacuum(self):
		self.__cur.execute("VACUUM;")
		self.__con.commit()
	
	# used to create extra indexes after scan.
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
		self.__queryCount += 1
		parentsRaw = self.__cur.execute("SELECT folder_id FROM folders WHERE parent IN(%s);" % query).fetchall()
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
	
	# Clears database and scans selected folders.
	def updateDataBase(self, paths: List[str]):
		print("Deleting data")
		self.__cur.executescript("""
			DELETE FROM files;
			DELETE FROM folders
		""")
		self.__con.commit()
		
		for Path in paths:
			# add folder and get its index
			self.__updateCount += 1
			self.__cur.execute("INSERT INTO folders (basename, folder_path)VALUES(?,?);", (Path, Path))
			print("scanning ", Path)
			self.__scan(Path, self.__getFolderIndex(Path))
		
		self.__con.commit()
		self.__createIndex()
		print("vacuuming")
		self.__vacuum()
		self.reportDbStats()
	
	# used in __scan to get the index of every directory it's added for the next
	def getIndexes(self, paths: List[str]) -> Dict[str, int]:
		query = self.__formatInQuery(paths)
		self.__queryCount += 1
		responses = self.__cur.execute(
			"SELECT folder_path, folder_id FROM folders WHERE folder_path IN(%s);" % query).fetchall()
		# move indexes to dict
		indexes = {}
		for response in responses:
			(Path, index, *rest) = response
			indexes[Path] = index
		return indexes
	
	# adds a folder without deleting the database
	# TODO implement check to see if path already exists
	def addFolder(self, Path: str):
		name = path.basename(Path)
		data = [(name, Path, self.__getFolderIndex(path.dirname(Path)))]
		self.__updateCount += 1
		self.__cur.executemany("INSERT INTO folders (basename,folder_path, parent) VALUES (?,?,?)", data)
		self.__scan(Path, self.__getFolderIndex(Path))
		self.__con.commit()
	
	# adds multiple folders without deleting the database
	def addFolders(self, paths: List[str]):
		for Path in paths:
			self.addFolder(Path)
	
	# for use outside this class to execute inserts/deletions
	# todo check if statement is complete
	def execute(self, script: str, commitOnCompletion: bool):
		self.__updateCount += 1
		self.__cur.executescript(script)
		if commitOnCompletion:
			self.__con.commit()
	
	# public function for queries
	def executeQuery(self, query: str) -> List[tuple]:
		self.__queryCount += 1
		return self.__cur.execute(query).fetchall()
	
	# returns a list of all files with specified extension
	def filesWithExtension(self, ext: (str, None)) -> List[tuple]:
		self.__queryCount += 1
		if ext is None:
			return self.__cur.execute("SELECT * FROM files WHERE extension IS NULL").fetchall()
		else:
			return self.__cur.execute("SELECT * FROM files WHERE extension = :ext", {"ext": ext}).fetchall()
	
	# removes a folder from the database
	def removeFolder(self, folder: str, cleanup: bool):
		index = self.__getFolderIndex(folder)
		directories = self.__getChildDirectories([index], True)
		directories.append(index)
		query = self.__formatInQuery(directories)
		
		self.__cur.execute("DELETE FROM files WHERE parent IN(%s)" % query)
		self.__cur.execute("DELETE FROM folders WHERE folder_id IN(%s)" % query)
		self.__updateCount += 2
		self.__con.commit()
		if cleanup:
			self.__vacuum()
	
	# counts the number of items inside the folder and all subfolders.
	# TODO rewrite this function to make use of recursion
	def countItems(self, folder: str):
		index = self.__getFolderIndex(folder)
		total = 0
		self.__queryCount += 1
		c = self.__cur.execute("SELECT COUNT(file_id) FROM files WHERE parent=:index", {"index": index}).fetchall()
		(count, *drop) = c[0]
		total += count
		
		children = self.__getChildDirectories([index], True)
		query = self.__formatInQuery(children)
		self.__queryCount += 1
		c = self.__cur.execute("SELECT COUNT(file_id) FROM files WHERE parent IN(%s)" % query).fetchall()
		(count, *drop) = c[0]
		total += count + len(children)
		children.clear()
		return total
	
	# fixme only works if you start from drive level
	# makes a copy of all folders and subfolders into refFolder
	def recreateFolderStructure(self, outFolder: str, refFolder: str):
		cleanOutput = refFolder.replace(":", "")
		try:
			mkdir(path.join(outFolder, cleanOutput))
		except FileNotFoundError:
			print('you should not see this: %s' % refFolder)
		
		index = self.__getFolderIndex(refFolder)
		children = self.__formatInQuery(self.__getChildDirectories([index], False))
		self.__queryCount += 1
		childDirs = self.__cur.execute("SELECT folder_path FROM folders WHERE folder_id IN(%s)" % children).fetchall()
		for child in childDirs:
			(direc, *drop) = child
			self.recreateFolderStructure(outFolder, direc)
	
	# similar to recreateFolderStructure but creates empty files as well.
	def recreateFileStructure(self, outFolder, refFolder):
		cleanOutput = refFolder.replace(":", "")
		outPath = path.join(outFolder, cleanOutput)
		try:
			mkdir(outPath)
		except FileNotFoundError:
			pass
		
		index = self.__getFolderIndex(refFolder)
		if index is not None:
			self.__queryCount += 1
			files = self.__cur.execute("SELECT basename FROM files WHERE parent = :index", {"index": index}).fetchall()
			for f in files:
				(file, *drop) = f
				try:
					open(path.join(outPath, file), 'x')
				except FileNotFoundError:
					continue
		
		children = self.__formatInQuery(self.__getChildDirectories([index], False))
		self.__queryCount += 1
		childDirs = self.__cur.execute("SELECT folder_path FROM folders WHERE folder_id IN(%s)" % children).fetchall()
		for child in childDirs:
			(direc, *drop) = child
			self.recreateFileStructure(outFolder, direc)
	
	# can be used to test private functions externally
	def testFunction(self):
		pass
	
	def vacuum(self):
		self.__vacuum()
	
	def reportDbStats(self):
		print("Queries:", str(self.__queryCount), " updates: ", str(self.__updateCount))
	
	def resetDbStats(self):
		self.__queryCount = 0
		self.__updateCount = 0
