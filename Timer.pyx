import time


class Timer:
	start = None
	
	def __init__(self):
		pass
	
	def startTime(self):
		self.start = time.time()
	
	def stopTime(self):
		return time.time() - self.start
