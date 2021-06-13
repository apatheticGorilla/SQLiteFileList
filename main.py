import databaseManager


# import Timer


def time_convert(sec):
	mins = sec // 60
	sec = sec % 60
	hours = mins // 60
	mins = mins % 60
	print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))


def exportTOCSV(header: str, path: str, data: list[tuple]):
	file = open(path)
	file.write(header + '\n')
	for d in data:
		line = ""
		for t in d:
			line = line + t + ','
		line = line[0:len(line) - 1]
		file.write(line)
	file.close()


db = databaseManager

# timer = Timer.Timer()
# timer.startTime()
# db.createDatabase()
# # , "D:\\", "E:\\", "G:\\", "F:\\"
drives = ["C:\\"]
# db.updateDataBase(drives)
# db.addFolder("C:\\")
# time_convert(timer.stopTime())
# db.executeQuery("CREATE INDEX size ON files(size)")
