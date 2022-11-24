import time


cdef class Timer:
	cdef int start

	def __init__(self):
		pass
		# self.start = None

	def startTime(self):
		self.start = time.time()

	def stopTime(self) -> float:
		return time.time() - self.start
