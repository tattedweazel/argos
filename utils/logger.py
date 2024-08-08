from datetime import datetime

class Logger:

	def log(self, msg, inline=False):
			"""
			like print, but better because timestamps and inline option
			"""
			if inline:
				print(f"{datetime.now()}: {msg}{' '*10}", end="\r")
			else:
				print(f"{datetime.now()}: {msg}")
