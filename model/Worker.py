#
#   @author: Nelson Antonio Antunes Junior
#   @email: nelson.a.antunes at gmail.com
#   @date: (DD/MM/YYYY) 08/02/2017

import ast

class Worker(object):
	"""docstring for Worker"""
	def __init__(self, hostname, username, path="", password="", pkey="", status="", active_time=0.0, failures=0, disconnection_time=0.0, connection_time=0):
		self.path = path
		self.hostname = hostname
		self.username = username
		self.password= password
		self.pkey = pkey
		self.status = status

		self.active_time = active_time
		self.failures = failures
		self.disconnection_time = disconnection_time
		self.connection_time = connection_time

	@staticmethod
	def decode(encoded_worker):
		worker_dict = ast.literal_eval(encoded_worker)

		worker = Worker(worker_dict["hostname"], worker_dict["username"], path= worker_dict["path"], password= worker_dict["password"], pkey= worker_dict["pkey"], status= worker_dict["status"])

		return worker

	def id(self):
		if self.path != "":
			return self.path.split("/")[-1]
		return None

	def __str__(self):
		return str({"path": self.path, "hostname": self.hostname, "username": self.username, "password": self.password, "pkey": self.pkey, "status": self.status})

	def encode(self):
		return str(self).encode()