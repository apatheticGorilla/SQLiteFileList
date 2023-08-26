import logging
import os
from datetime import datetime
from os import listdir, path, mkdir
from sqlite3 import connect, OperationalError, complete_statement
# noinspection PyMethodMayBeStatic
from typing import List
import numpy as np
cimport cython

@cython.linetrace(True)
cdef class databaseManager:
	cdef log
	cdef __con
	cdef __cur
	cdef int __queryCount
	cdef int __updateCount
	cdef int __currentDepth
	cdef int __maxDepth
	def __init__(self, Path: str, logPath: str):
		# rename old log if found
		if path.exists(logPath):
			timestamp = datetime.fromtimestamp(path.getmtime(logPath)).strftime('%Y_%m_%d-%H-%M-%S')
			newname = path.join(path.dirname(logPath), timestamp + '_' + path.basename(logPath))
			os.rename(logPath, newname)

		# configure logger
		self.log = logging.getLogger('DatabaseManager')
		self.log.setLevel(logging.DEBUG)

		ch = logging.StreamHandler()
		ch.setLevel(logging.INFO)

		fh = logging.FileHandler(filename=logPath, encoding="UTF_8")
		fh.setLevel(logging.DEBUG)

		fmat = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		ch.setFormatter(fmat)
		fh.setFormatter(fmat)

		self.log.addHandler(ch)
		self.log.addHandler(fh)

		dbExists = path.exists(Path)
		self.log.info('initializing connection object')
		self.__con = connect(Path)
		self.__cur = self.__con.cursor()
		self.__queryCount = 0
		self.__updateCount = 0
		self.__currentDepth = 0
		self.__maxDepth = -1

		# create database if it doesn't exist already
		if not dbExists:
			self.log.info("No database file was found, creating one now")
			self.__createDatabase()
		self.log.info('databaseManager is ready to go')

	cdef str __formatInQuery(self, clauses: [str]):
		sanitized_clauses = []
		for clause in clauses:
			clean = str(clause).replace('"', '""')
			sanitized_clauses.extend(['"', clean, '",'])
		query = ''.join(sanitized_clauses)
		return query[0:len(query) - 1]

	# get file info for the database
	cdef tuple __getFileInfo(self, basename: str, filepath: str, parent: (int, None)):
		size = 0
		# TODO there's probably a tool for this that's not being used
		try:
			name = path.basename(filepath)
			extension = name[name.rindex('.'):]
		except ValueError:
			extension = None
		try:
			size = path.getsize(filepath)
		except FileNotFoundError:
			self.log.warning("File Not Found: %s", filepath)

		return basename, filepath, extension, size, parent

	# used to recursively scan a selected folder
	cdef __scan(self, Path: str, parent: (int, None)):
		self.__currentDepth += 1
		if 0 < self.__maxDepth <= self.__currentDepth:
			self.__currentDepth -= 1
			self.log.debug('exceeded maximum depth on folder: %s', Path)
			return

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
				self.log.warning("Folder/file not found: %s", item)

		# add items to database
		self.__updateCount += 1
		self.__cur.executemany("INSERT INTO files (basename,file_path,extension,size,parent) VALUES(?,?,?,?,?)", fileData)
		self.__updateCount += 1
		self.__cur.executemany("INSERT INTO folders (basename,folder_path, parent) VALUES (?,?,?)", dirData)

		parents = self.__getIndexes(directories)

		# rinse and repeat
		dirData.clear()
		fileData.clear()
		for directory in directories:
			if path.islink(directory):
				self.log.debug("%s is a systemic link", directory)
				continue
			try:
				self.__scan(directory, parents[directory])
			except PermissionError:
				# we must subtract the current depth when these exceptions are thrown
				# because it did not reach the end
				self.__currentDepth -= 1
				self.log.warning("permission denied: %s", directory)

			except FileNotFoundError:
				self.__currentDepth -= 1
				self.log.warning("could not find: %s", directory)

			except OSError:
				self.__currentDepth -= 1
				self.log.warning("OS error on: %s", directory)

		directories.clear()
		self.__currentDepth -= 1
		assert self.__currentDepth >= 0

	cdef str __getFolderIndex(self, Path: str):
		try:
			result = self.__cur.execute("SELECT rowid FROM folders WHERE folder_path =:Path", {"Path": Path}).fetchall()
			self.__queryCount += 1
			# folder_path is unique, so it would be incredible if this failed before a database insertion
			assert len(result) <= 1

			if len(result) == 0:
				return None

			(ID, *rest) = result[0]
			return str(ID)
		except OperationalError:
			self.log.error("failed to get index for directory: %s", Path)
			return None

	cdef __vacuum(self):
		self.__cur.execute("VACUUM;")
		self.__con.commit()

	# used to create extra indexes after scan.
	cdef __createIndex(self):
		self.__cur.executescript("""
		CREATE INDEX IF NOT EXISTS extension ON files(extension);
		CREATE INDEX IF NOT EXISTS size ON files(size);
		CREATE INDEX IF NOT EXISTS file_parent ON files(parent);
		CREATE INDEX IF NOT EXISTS folder_parent ON folders(parent);
		""")
		self.__con.commit()

	# searches for and finds folders within given folders, can be used recursively
	cdef list __getChildDirectories(self, folders: list, searchRecursively: bool):
		query = self.__formatInQuery(folders)
		self.__queryCount += 1
		parentsRaw = self.__cur.execute("SELECT rowid FROM folders WHERE parent IN(%s);" % query).fetchall()

		children = []
		for p in parentsRaw:
			(ID, *drop) = p
			children.append(ID)

		if len(children) > 0 and searchRecursively:
			children.extend(self.__getChildDirectories(children, True))
		return children

	cdef __createDatabase(self):
		self.log.info('(Re)Creating Tables')
		self.__cur.executescript("""
		DROP TABLE IF EXISTS files;
		DROP TABLE IF EXISTS folders;
		DROP INDEX IF EXISTS folder_path;
		CREATE TABLE files(
			basename TEXT,
			file_path TEXT,
			extension TEXT,
			size INT,
			parent INT,
			FOREIGN KEY (parent) REFERENCES folders (rowid)
			);
		CREATE TABLE folders(
			basename TEXT,
			folder_path TEXT,
			parent INT,
			FOREIGN KEY (parent) REFERENCES folders(rowid)
		);
		CREATE UNIQUE INDEX folder_path ON folders(folder_path);
		VACUUM;
		""")
		self.__con.commit()

	def createDatabase(self):
		"""
		Creates the database tables
		"""
		self.__createDatabase()

	# Clears database and scans selected folders.
	cdef __updateDataBase(self, paths: List[str], maxSearchDepth):
		self.__maxDepth = maxSearchDepth
		self.log.info('Deleting data')
		self.__cur.executescript("""
			DELETE FROM files;
			DELETE FROM folders
		""")
		self.__con.commit()

		for Path in paths:
			# add folder and get its index
			if path.exists(Path):
				self.__updateCount += 1
				self.__cur.execute("INSERT INTO folders (basename, folder_path)VALUES(?,?);", (Path, Path))
				self.log.info('scanning %s', Path)

				self.__scan(Path, self.__getFolderIndex(Path))
				assert self.__currentDepth == 0
			else:
				self.log.error("Could not find folder '%s'", Path)

		self.__con.commit()
		self.__createIndex()
		self.log.info('vacuuming')
		self.__vacuum()
		self.__reportDbStats()

	def updateDatabase(self, paths: List[str], maxSearchDepth):
		"""
		Clears the database and then scans and adds all files and folders in the given folder paths.
		:param paths: List of folders to scan.
		:param maxSearchDepth: The maximum recursion depth when scanning folders.
		"""
		self.__updateDataBase(paths, maxSearchDepth)

	# used in __scan to get the index of every directory it's added for the next
	cdef __getIndexes(self, paths: List[str]):
		query = self.__formatInQuery(paths)
		self.__queryCount += 1
		responses = self.__cur.execute(
			"SELECT folder_path, rowid FROM folders WHERE folder_path IN(%s);" % query).fetchall()

		# move indexes to dict
		indexes = {}
		for response in responses:
			(Path, index, *rest) = response
			indexes[Path] = index
		return indexes

	# adds a folder without deleting the database
	cdef __addFolder(self, Path: str, maxSearchDepth: int):
		# check if folder is in database
		response = self.__cur.execute("SELECT rowid FROM folders WHERE folder_path=:filepath;", {"filepath": Path}).fetchall()
		if not len(response) == 0:
			self.log.error("The folder:" + Path + " already exists in the database")
			return

		self.__maxDepth = maxSearchDepth
		name = path.basename(Path)
		data = [(name, Path, self.__getFolderIndex(path.dirname(Path)))]

		self.__updateCount += 1
		self.__cur.executemany("INSERT INTO folders (basename,folder_path, parent) VALUES (?,?,?)", data)
		self.__scan(Path, self.__getFolderIndex(Path))
		self.__con.commit()

	def addFolder(self, Path: str, maxSearchDepth: int):
		"""
		Adds a folder to the database.
		:param Path: the file path of the folder to add.
		:param maxSearchDepth: The maximum recursion depth when scanning folders.
		"""
		self.__addFolder(Path, maxSearchDepth)

	# adds multiple folders without deleting the database
	cdef __addFolders(self, paths: List[str], maxSearchDepth: int):
		for Path in paths:
			self.__addFolder(Path, maxSearchDepth)

	def addFolders(self, paths: List[str], maxSearchDepth: int):
		"""
		Adds multiple folders to the database in bulk.
		:param paths: A List of file paths.
		:param maxSearchDepth: The maximum recursion depth when scanning folders.
		"""
		self.__addFolders(paths, maxSearchDepth)

	# for use outside this class to execute inserts/deletions
	cdef __execute(self, script: str, commitOnCompletion: bool):
		if not complete_statement(script):
			self.log.error('execute: incomplete sql statement')
			return
		self.__updateCount += 1
		self.__cur.executescript(script)
		if commitOnCompletion:
			self.__con.commit()

	def execute(self, script: str, commitOnCompletion: bool):
		"""
		Executes an SQLite script on the database. This does not return anything, for queries use executeQuery().
		:param script: The SQLite script to execute.
		:param commitOnCompletion: whether to commit after running the script.
		"""
		self.__execute(script, commitOnCompletion)

	# public function for queries
	cdef list __executeQuery(self, query: str):
		if not complete_statement(query):
			self.log.error('executeQuery: incomplete sql statement')
			return None
		self.__queryCount += 1
		return self.__cur.execute(query).fetchall()

	def executeQuery(self, query: str):
		"""
		Used to query the database outside of this class.
		:param query: The SQLite query.
		:return: The query results.
		"""
		return self.__executeQuery(query)

	# returns a list of all files with specified extension
	cdef list __filesWithExtension(self, ext: (str, None)):
		self.__queryCount += 1
		if ext is None:
			return self.__cur.execute("SELECT * FROM files WHERE extension IS NULL").fetchall()
		else:
			return self.__cur.execute("SELECT * FROM files WHERE extension = :ext", {"ext": ext}).fetchall()

	def filesWithExtension(self, ext: (str, None)):
		"""
		searches database for all files with the given file extension.
		:param ext: The file extension such as ".txt" or None for files with no extension.
		:return: A list of tuples for matching database records.
		"""
		return self.__filesWithExtension(ext)

	# removes a folder from the database
	cdef __removeFolder(self, folder: str, cleanup: bool):
		self.log.info("getting index for folder...")
		index = self.__getFolderIndex(folder)

		self.log.info("Getting Child directories...")
		directories = self.__getChildDirectories([index], True)
		directories.append(index)

		self.log.info("formatting query...")
		query = self.__formatInQuery(directories)

		self.log.info("Deleting Folders...")
		self.__cur.execute("DELETE FROM files WHERE parent IN(%s)" % query)
		self.log.info("Deleting files...")
		self.__cur.execute("DELETE FROM folders WHERE rowid IN(%s)" % query)
		self.__updateCount += 2
		self.__con.commit()
		if cleanup:
			self.log.info("vacuuming...")
			self.__vacuum()

	def removeFolder(self, folder: str, cleanup: bool):
		"""
		Removes a folder and all of its children from the database.
		:param folder: The file path of the folder to remove.
		:param cleanup: whether to run vacuum() after completion.
		"""
		self.__removeFolder(folder, cleanup)

	# counts the number of items inside the folder and all sub-folders.
	# TODO rewrite this function to make use of recursion
	cdef int __countItems(self, folder: str):
		index = self.__getFolderIndex(folder)
		total = 0
		self.__queryCount += 1
		c = self.__cur.execute("SELECT COUNT(rowid) FROM files WHERE parent=:index", {"index": index}).fetchall()
		(count, *drop) = c[0]
		total += count

		children = self.__getChildDirectories([index], True)
		query = self.__formatInQuery(children)
		self.__queryCount += 1
		c = self.__cur.execute("SELECT COUNT(rowid) FROM files WHERE parent IN(%s)" % query).fetchall()
		(count, *drop) = c[0]
		total += count + len(children)
		children.clear()
		return total

	def countItems(self, folder: str):
		"""
		counts all the files and folders in a given folder based on what's stored in the database.
		:param folder: The folder to count from.
		:return: The number of items in the folder.
		"""
		return self.__countItems(folder)

	# makes a copy of all folders and sub-folders into refFolder
	cdef __recreateFolderStructure(self, outFolder: str, refFolder: str):
		# get basename and append to reference directory
		b = self.__cur.execute("SELECT basename FROM folders WHERE folder_path =:path", {"path": refFolder}).fetchall()
		self.__queryCount += 1
		(basename, *d) = b[0]
		cleanOutput = basename.replace(":", "")
		# edge case for linux root
		if refFolder == '/':
			target = path.join(outFolder, 'root')
		else:
			target = path.join(outFolder, cleanOutput)

		try:
			mkdir(target)
		except FileNotFoundError:
			self.log.warning('failed to make directory: %s' % refFolder)

		index = self.__getFolderIndex(refFolder)
		children = self.__formatInQuery(self.__getChildDirectories([index], False))
		# get all folders and recursively create structure
		self.__queryCount += 1
		childDirs = self.__cur.execute("SELECT folder_path FROM folders WHERE rowid IN(%s)" % children).fetchall()
		for child in childDirs:
			(direc, *drop) = child
			self.__recreateFolderStructure(target, direc)

	def recreateFolderStructure(self, outFolder: str, refFolder: str):
		"""
		Finds all sub-folders inside the reference folder and writes the structure to the output folder.
		:param outFolder: The file path that the structure is written to.
		:param refFolder: The folder to mimic the structure of.
		"""
		self.__recreateFolderStructure(outFolder, refFolder)

	# similar to recreateFolderStructure but creates empty files as well.
	cdef __recreateFileStructure(self, outFolder, refFolder):
		# get basename and append to target directory
		b = self.__cur.execute("SELECT basename FROM folders WHERE folder_path =:path", {"path": refFolder}).fetchall()
		self.__queryCount += 1
		(basename, *d) = b[0]
		cleanOutput = basename.replace(":", "")
		# edge case for linux root
		if refFolder == '/':
			target = path.join(outFolder, 'root')
		else:
			target = path.join(outFolder, cleanOutput)

		try:
			mkdir(target)
		except FileNotFoundError:
			self.log.warning('failed to make directory: %s' % refFolder)

		# get files in folder, if any
		index = self.__getFolderIndex(refFolder)
		# create empty file where it would be
		if index is not None:
			self.__queryCount += 1
			files = self.__cur.execute("SELECT basename FROM files WHERE parent = :index", {"index": index}).fetchall()
			for f in files:
				(file, *drop) = f
				pth = path.join(target, file)
				try:
					open(pth, 'x')
				except FileNotFoundError:
					self.log.warning('Failed to create file: %s', pth)

		# get all folders and recursively create structure
		children = self.__formatInQuery(self.__getChildDirectories([index], False))
		self.__queryCount += 1
		childDirs = self.__cur.execute("SELECT folder_path FROM folders WHERE rowid IN(%s)" % children).fetchall()
		for child in childDirs:
			(direc, *drop) = child
			self.__recreateFileStructure(target, direc)

	def recreateFileStructure(self, outFolder, refFolder):
		"""
		Finds all files sub-folders inside the reference folder and writes the structure to the output folder using empty files.
		:param outFolder: The file path that the structure is written to.
		:param refFolder: The folder to mimic the structure of.
		"""
		self.__recreateFileStructure(outFolder, refFolder)

	# can be used to test private functions externally
	# NOTE: any changes to this function should not be committed!
	def testFunction(self):
		pass

	def vacuum(self):
		"""
		Runs vacuum command on the database.
		"""
		self.__vacuum()

	cdef __reportDbStats(self):
		self.log.info("Queries: %s Updates: %s", str(self.__queryCount), str(self.__updateCount))

	def reportDbStats(self):
		"""
		Logs database statistics to the info channel
		"""
		self.__reportDbStats()

	cdef __resetDbStats(self):
		self.__queryCount = 0
		self.__updateCount = 0

	def resetDbStats(self):
		"""
		Resets the queryCount and UpdateCount to 0
		"""
		self.__resetDbStats()

	cdef int __AvgFileSize(self, folder: str):
		folders = self.__getChildDirectories([self.__getFolderIndex(folder)], True)
		response = self.__cur.execute(
			"SELECT AVG(size) FROM files WHERE parent IN(%s)" % self.__formatInQuery(folders)).fetchall()
		(average, *drop) = response[0]
		return average

	def AvgFileSize(self, folder: str):
		"""
		Calculates the average size of the files in the given folder.
		:param folder: The folder to calculate.
		:return: The average size of the files.
		"""
		return self.__AvgFileSize(folder)

	cdef int __MedianFileSize(self, folder: str):
		folders = self.__getChildDirectories([self.__getFolderIndex(folder)], True)
		response = self.__cur.execute(
			"SELECT size FROM files WHERE parent IN(%s)" % self.__formatInQuery(folders)).fetchall()
		responses = []
		for r in response:
			(size, *drop) = r
			responses.append(size)
		return np.median(responses)

	def MedianFileSize(self, folder: str):
		"""
		Calculates the median size of the files in the given folder.
		:param folder: The folder to calculate.
		:return: The median file size.
		"""
		return self.__MedianFileSize(folder)
