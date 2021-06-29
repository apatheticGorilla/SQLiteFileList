import databaseManager


# import Timer


def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))


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


db = databaseManager

# timer = Timer.Timer()
# timer.startTime()
# db.createDatabase()
# # , "D:\\", "E:\\", "G:\\", "F:\\"
drives = ["C:\\", "D:\\", "G:\\", "F:\\", "z:\\"]

# db.addFolder("Z:\\")
# db.vacuum()
# db.updateDataBase(drives)
simpleFile = open("C:\\temp\\files.tfl", 'w', encoding="UTF-8")
fileList = db.executeQuery("SELECT file_path FROM files WHERE size <= 4096")
for f in fileList:
	(line, *drop) = f
	simpleFile.write(line + "\n")
simpleFile.close()
# db.addFolder("C:\\")
# time_convert(timer.stopTime())
# files = db.executeQuery("SELECT * FROM files")
# files = db.filesWithExtension(".ttf")
# exportTOCSV("file_id,path,extension,size,parent", "C:\\Temp\\all.csv", files)


