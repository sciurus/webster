#!/usr/bin/python

import logging
import os
import platform
import re
import subprocess
import telnetlib
import time

class WebsterClassifier:

	next_port = 2000

	def __init__(self, category, port=None, uniform=False):
		self.logger = logging.getLogger('')
		self.category = category
		self.host = 'localhost'
		self.uniform = uniform

		if port:
			self.port = port
		else:
			self.port = self.__class__.next_port
			self.__class__.next_port += 1

		self.startup()

	
	def __del__(self):
		self.shutdown()

	def startup(self):

		if platform.architecture()[0] == '32bit':
			model_path = "/path/to/bow_models/%s/" % self.category
		else:
			model_path = "/path/to/bow_models_64/%s/" % self.category
		
		command = ['rainbow', '--verbosity=1', '-d', model_path, "--score-precision=2", "--skip-html", "--query-server=%d" % self.port]
		if self.uniform == True:
			command.append('--uniform-class-priors')
		self.logger.debug("Starting rainbow on port %d" % self.port)
		self.process = subprocess.Popen(command, stdout=open("%d.log" % self.port, 'w'), stderr=subprocess.STDOUT)

		self.rainbow_connection = None
		while self.rainbow_connection == None:
			try:
				self.logger.debug("Trying to connect to rainbow on %s" % self.port)
				self.rainbow_connection = telnetlib.Telnet(self.host, self.port)
			except:
				if self.process.poll() != None:
					raise WebsterClassifierError("Rainbow on %s exited with %s" % (self.port, self.process.returncode))
			time.sleep(10)

	def shutdown(self):
		if not self.rainbow_connection == None:
			self.rainbow_connection.close()
		self.logger.debug('Shutting down rainbow')
		if hasattr(self.process, 'terminate'):
			self.process.terminate()
		else:
			subprocess.call(['kill', "%d" % self.process.pid])
		self.process.wait()

	def classify(self, document):
		result = None
		while result == None:
			try:
				self.rainbow_connection.write(document)
				# control sequence to signal end of document
				self.rainbow_connection.write("\r\n.\r\n")
				result = self.rainbow_connection.read_some()
			except:
				self.logger.warn("Restarting rainbow on %s" % self.port)
				self.shutdown()
				self.startup()

		pattern = re.compile(r"%s (\d+\.*\d*)" % self.category)
		match = re.search(pattern, result)

		try:		
			probability = match.group(1)
			score = float(probability)
			# rainbow returns really low scores in scientific notation
			# the code above will mistakenly convert those into a score greater than 1
			# this corrects for that
			if score > 1.0:
				return 0.0
			else:
				return score
		except:
			raise WebsterClassifierError("Classifying via rainbow on %s failed. Result was %s" % (self.port, result))

class WebsterClassifierError(Exception):
    """Exception raised for errors in communication with classifier."""
    pass
