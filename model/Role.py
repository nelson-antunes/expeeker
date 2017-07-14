#
#	@author: Nelson Antonio Antunes Junior
#	@email: nelson.a.antunes at gmail.com
#	@date: (DD/MM/YYYY) 17/02/2017

import ast

class Role(object):
	"""docstring for Role"""
	def __init__(self, name, parameters, no_workers, role_id=""):
		self.name = name
		self.parameters = parameters
		self.id = role_id
		self.no_workers = no_workers
		
	@staticmethod
	def decode(encoded_role):
		
		role_dict = ast.literal_eval(encoded_role)

		role = Role(role_dict["name"], role_dict["parameters"], role_dict["no_workers"], role_dict["id"])

		return role

	def __str__(self):
		return str({"name": self.name, "parameters": self.parameters, "no_workers": self.no_workers, "id": self.id})

	def encode(self):
		return str(self).encode()