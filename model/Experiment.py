#
#	@author: Nelson Antonio Antunes Junior
#	@email: nelson.a.antunes at gmail.com
#	@date: (DD/MM/YYYY) 13/02/2017

import shutil, ast, os

from model.Role import Role

class Experiment(object):
	"""docstring for Experiment"""

	class Actor(object):
		def __init__(self):
			self.path = ''
			self.role_id = ''			

	def __init__(self, name, filename, roles, is_snapshot, exp_id="", actors={}):
		self.name = name
		self.filename = filename
		self.roles = roles
		self.is_snapshot = is_snapshot
		self.id = exp_id
		self.actors = actors
		self.actor = self.Actor()
		
	def save_file(self, fileobj):
		shutil.copyfileobj(fileobj, open(os.path.expanduser("~/controller/experiments/%s" % self.filename), 'w'))

	@staticmethod
	def decode(encoded_exp):
		
		exp_dict = ast.literal_eval(encoded_exp)

		roles = []
		for role in exp_dict["roles"]:
			roles.append(Role.decode(role))

		exp = Experiment(exp_dict["name"], exp_dict["filename"], roles, exp_dict["is_snapshot"], exp_dict["id"])

		return exp

	def __str__(self):
		roles_str = []
		for role in self.roles:
			roles_str.append(str(role))

		return str({"name": self.name, "filename": self.filename, "roles": roles_str, "is_snapshot": self.is_snapshot, "id": self.id})

	def encode(self):
		return str(self).encode()
