#
#   @author: Nelson Antonio Antunes Junior
#   @email: nelson.a.antunes at gmail.com
#   @date: (DD/MM/YYYY) 27/01/2017

import kazoo, traceback, subprocess, threading, imp, os, time
from kazoo.client import *
from worklib.Snapshot import *

class Printer:
	def __init__(self):
		self.fds = {}
		self.default = open('file_not_found.txt','a+',1)

	def write(self,value):
		try:
			self.fds[threading.currentThread()].write(value)
		except:
			self.default.write(value)

	def add(self,fd):
		self.fds[threading.currentThread()] = fd

#STATIC PATHS
class PATHS(object):

	CONNECTED_FREE = "/connected/free_workers"
	CONNECTED_BUSY = "/connected/busy_workers"
	DISCONNECTED = '/disconnected/workers'
	REGISTERED_WORKERS = "/registered/workers/"

class Experiment(object):
	def __init__(self, exp_path, exp_name, exp_parameters, exp_actor_id, is_snapshot):
		self.path = exp_path
		self.name = exp_name
		self.parameters = exp_parameters
		self.popen = None
		self.actor_id = exp_actor_id
		self.worker_torun_id = ''
		self.is_snapshot = is_snapshot
		self.snapshot = Snapshot()

	def run(self, wclient):
		if self.is_snapshot:
			try:
				exp_mod = imp.load_source('Actor','experiments/%s/Actor.py' % self.name)
				self.snapshot = exp_mod.Actor()
				self.snapshot.config(wclient, self.path, self.actor_id, 'experiments/%s/' % self.name)
				self.popen = threading.Thread(target=self.snapshot.start, args= (self.parameters, 'experiments/%s/%s.' % (self.name,self.name)))
				self.popen.daemon = True
				self.popen.start()
			except:
				traceback.print_exc()
				self.snapshot.poll = -2
		else:
			try:
				self.popen = subprocess.Popen(["cd", "experiments/%s;" % self.name, "%s" % self.parameters, "1>%s.out" % self.name, "2>%s.err" % self.name])
				self.poll = self.popen.poll
			except:
				self.poll = -2

	def is_running(self):
		if self.popen:
			return self.popen.is_alive()
		return False

	def is_finished(self):
		if self.is_snapshot:
			return self.snapshot.poll != None
		else:
			return self.popen.poll() != None 
	def is_started(self):
		return self.popen

class WorkerClient(object):
	def __init__(self, zk_addr, worker_hostname=''):
		self.zk = KazooClient(zk_addr, connection_retry= kazoo.retry.KazooRetry(max_tries=-1, max_delay= 250))
		self.zk_addr = zk_addr
		self.hostname = worker_hostname
		self.worker_path = PATHS.REGISTERED_WORKERS + worker_hostname
		self.reregister = True
		self.busy = None
		self.connection = None
		self.connection_timeout = 0
		self.zk.start()

	def connected(self):
		if self.connection == None:
			return False
		if self.zk.exists(self.connection):
			return True
		self.connection = None
		return False

	@staticmethod
	def load_config_file(filepath):
		cfg = {}
		f = open(filepath,"r")
		for l in f.readlines():
			opt, arg = l.split("=")
			cfg[opt] = arg[:-1]
		return cfg

	def worker_active_time_uptade(self,adding_time):
		active_time = float(self.zk.get("%s/active_time" % self.worker_path)[0])
		self.zk.set("%s/active_time" % self.worker_path, value= str(active_time+adding_time).encode())	

	def worker_keep_alive(self, time, busy=False):
		connected = False
		try:
			connected = self.connected()
		except:
			pass

		if connected:
			self.worker_active_time_uptade(time)

		if (not self.connection) or busy != self.busy:

			connection_path = "%s/%s" % (PATHS.CONNECTED_BUSY if busy else PATHS.CONNECTED_FREE, self.hostname)
			delete_path = "%s/%s" % (PATHS.CONNECTED_BUSY if not busy else PATHS.CONNECTED_FREE, self.hostname)
			
			try:
				self.zk.create(connection_path, value=self.worker_path.encode(), ephemeral=True)  
				self.zk.set("%s/connection" % self.worker_path, value= connection_path.encode())
			except:
				pass

			try:
				self.zk.delete(delete_path)
			except:
				pass

			try:
				self.zk.delete("%s/%s" % (PATHS.DISCONNECTED, self.hostname), recursive=True)
			except:
				pass

			self.connection = connection_path
			self.busy = busy

		return self.connection

	def watch_new_exp(self):
		kazoo.recipe.watchers.ChildrenWatch(self.zk, "%s/torun" % self.worker_path, self.exp_handler)


	def exp_get(self, exp_path):
		exp_name,_ = self.zk.get(exp_path)

		exp_cfg = WorkerClient.load_config_file("experiments/%s/info.cfg" % exp_name)

		return Experiment(exp_path, exp_name, exp_cfg["parameters"], exp_cfg["actor_id"], eval(exp_cfg["is_snapshot"]))

	def exp_ready(self, exp_obj):
		wc = WorkerClient(self.zk_addr)
		@self.zk.DataWatch('%s/start' % exp_obj.path)
		def ready(data,stat):
			if data:
				exp_obj.run(wc)
				return False

		
	def exp_finished(self, exp_obj):
		filename = "experiments/%s/%s." % (exp_obj.name, exp_obj.name)
		code_output = exp_obj.popen.poll() if not exp_obj.is_snapshot else exp_obj.snapshot.poll
		output = '(%i): ' % code_output
		try:
			f_output = open(filename+'out', 'r+')
			f_error = open(filename+'err', 'r+')
			error = f_error.read()
		
			output += '%s' % f_output.read()
			if error != '':
				output += '\nerror: ' + error
		except:
			output += 'Unable to run experiment'

		try:
			self.zk.create("%s/actors/%s/output" % (exp_obj.path, exp_obj.actor_id), value= output.encode())
			
			self.zk.delete("%s/torun/%s" % (self.worker_path, exp_obj.worker_torun_id), recursive=True)
		except:
			pass

		self.current_experiments.remove(exp_obj)
		

	def exp_handler(self,exp_diff):
		try:
			exp_new = [n for n in exp_diff if n not in self.current_experiments]
			
			for exp_id in exp_new:
				if self.zk.exists("%s/torun/%s" % (self.worker_path, exp_id)):
					#not deleted
					exp_path,_ = self.zk.get("%s/torun/%s" % (self.worker_path, exp_id))
					exp_obj = self.exp_get(exp_path)
					exp_obj.worker_torun_id = exp_id
					self.current_experiments.append(exp_obj)
					self.exp_ready(exp_obj)
		except:
			traceback.print_exc()

	def exp_load(self):
		self.current_experiments = []	#Experiment objects

		self.watch_new_exp()


	def snap_get(self, actor_path):
		if self.zk.exists("%s/data" % actor_path):
			s,_ = self.zk.get("%s/data" % actor_path)
			return eval(s)
		
		return None

	def snap_set(self, actor_path, value):
		if self.zk.exists("%s/data" % actor_path): 
			self.zk.set("%s/data" % actor_path, str(value).encode())
		else:
			self.zk.create("%s/data" % actor_path, str(value).encode())
