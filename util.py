import databaseManager
from matplotlib import pyplot
import numpy as np


def exportToCSV(header: str, path: str, data: list[tuple]):
	file = open(path, 'w', encoding="UTF-16")
	file.write(header + '\n')
	for d in data:
		line = ""
		for t in d:
			line = line + str(t) + ','
		line = line[0:len(line) - 1]
		file.write(line + "\n")
	file.close()
	print("exported to:", path)


def pieChartByExtension(min):
	db = databaseManager
	extensions = []
	counts = []
	for item in db.executeQuery("SELECT extension FROM files GROUP BY(extension) ORDER BY count(file_id) DESC"):
		(ext, *drop) = item
		if ext is None:
			extensions.append("None")
			query = db.executeQuery("SELECT count(file_id) FROM files WHERE extension IS NULL")
			(count, *drop) = query[0]
			counts.append(count)
		else:
			extensions.append(ext)
			query = db.executeQuery("SELECT COUNT(file_id) FROM files WHERE extension = '" + ext + "'")
			(count, *drop) = query[0]
			counts.append(count)
	finalExtensions = []
	finalCounts = []
	other = 0
	for i in range(len(counts)):
		if counts[i] < min:
			other += counts[i]
		else:
			finalExtensions.append(extensions[i])
			finalCounts.append(counts[i])
	finalExtensions.append("other")
	finalCounts.append(other)
	y = np.array(finalCounts)
	pyplot.pie(y, labels=finalExtensions)
	pyplot.show()


def barGraphByExtension(min):
	db = databaseManager
	extensions = []
	counts = []
	for item in db.executeQuery("SELECT extension FROM files GROUP BY(extension)"):
		(ext, *drop) = item
		if ext is None:
			extensions.append("None")
			query = db.executeQuery("SELECT count(file_id) FROM files WHERE extension IS NULL")
			(count, *drop) = query[0]
			counts.append(count)
		else:
			extensions.append(ext)
			query = db.executeQuery("SELECT COUNT(file_id) FROM files WHERE extension = '" + ext + "'")
			(count, *drop) = query[0]
			counts.append(count)
	finalExtensions = []
	finalCounts = []
	other = 0
	for i in range(len(counts)):
		if counts[i] < min:
			other += counts[i]
		else:
			finalExtensions.append(extensions[i])
			finalCounts.append(counts[i])
	finalExtensions.append("other")
	finalCounts.append(other)
	y = np.array(finalCounts)
	x = np.array(finalExtensions)
	pyplot.bar(x, y)
	pyplot.show()


def writeTFLList(path: str, WhereClause: str):
	db = databaseManager
	simpleFile = open(path, 'w', encoding="UTF-8")
	fileList = db.executeQuery(
		"SELECT file_path FROM files WHERE " + WhereClause)
	for f in fileList:
		(Line, *drop) = f
		simpleFile.write(Line + "\n")
	simpleFile.close()
