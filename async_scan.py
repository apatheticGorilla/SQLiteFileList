from os import path
from multiprocessing import Pool, freeze_support


class async_scan:
	freeze_support()
	
	def async_scan(self, info):
		(item, Path, parent) = info
		try:
			filepath = path.join(Path, item)
			if path.isfile(filepath):
				return self.__compileFileData(item, filepath, parent)
			else:
				return filepath
		
		except FileNotFoundError:
			print("file not found: ", item)
		return None
	
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
	
	def scan(self, items):
		fileData = []
		directories = []
		dirData = []
		(i, p, parent) = items[0]
		with Pool(16) as p:
			for i in p.imap_unordered(self.async_scan, items):
				if type(i) is tuple:
					fileData.append(i)
				elif i is not None:
					directories.append(i)
					dirData.append((path.basename(i), i, parent))
		return fileData, directories, dirData
