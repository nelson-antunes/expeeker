#
#	@author: Nelson Antonio Antunes Junior
#	@email: nelson.a.antunes at gmail.com
#	@date: (DD/MM/YYYY) 24/01/2017

import sys, os, socket, logging, time, multiprocessing, subprocess, signal

from conlib.controller_client import *
from conlib.remote_access import Channel

from extralib.daemon import Daemon
from model.Worker import Worker
from model.Experiment import Experiment
from model.Role import Role

logging.basicConfig()

_controllerport = "2181"
_pyvers = "2.7.13"
_timeout = 10
_worker_daemon = "daemon_worker.py"
_worklibtarfile = "worklib.tar.gz"
_local_experiments_dir = os.path.expanduser("~/controller/experiments/")

def get_ip():
	return [(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]

_logging_interval = 30
_realocate_timeout = 240
_restart_timeout = _realocate_timeout/2


#RPM = Resource Pool Manager
class RPM(multiprocessing.Process):
	def run(self):
		cclient = ControllerClient()
		exit = False

		workers = cclient.worker_get_all()
		workers_disconnected = set(filter(lambda x: x.status != 'BUSY' or x.status != 'IDLE',workers))
		workers = set(workers)

		starting_time = time.time()
		
		last= starting_time
		while not exit:
			now = time.time()
			try:
				with open("rpm_activity.log","w+") as f:
					print >> f, "%16s, \t%50s, \t%13s, \t%24s, \t%24s" %('TEMPO ATIVO', 'HOSTNAME', 'ESTADO', 'ULTIMA VEZ QUE LIGOU', 'QUANDO DESCONECTOU')
					for worker in sorted(cclient.worker_get_all(), key=lambda x: x.hostname):
						

						if worker.status == 'BUSY' or worker.status == 'IDLE':
						
							if worker.hostname in workers_disconnected or not (worker in workers):
								cclient.worker_update_connection_time(worker.path, now)
								workers_disconnected.discard(worker.hostname)

						else:
							workers_disconnected.add(worker.hostname)
							if worker.status == 'LOST BUSY' and now - worker.disconnection_time > _realocate_timeout:
								cclient.worker_add_disconnected(worker.hostname, 'RECOVERING')
								cclient.task_add(COMMANDS.RECOVER_ACTOR, worker= worker)
								
							elif worker.status == 'NEW LOST IDLE':
								cclient.worker_add_disconnected(worker.hostname, 'LOST IDLE')

							elif worker.status == 'NEW LOST BUSY':
								cclient.worker_add_disconnected(worker.hostname, 'LOST BUSY')
								#cclient.task_add(COMMANDS.START_WORKER, worker= worker)
						
						workers.add(worker)
						
						dcnx_time = ''
						if worker.disconnection_time != 0:
							dcnx_time = time.ctime(worker.disconnection_time)
						
						last_login = ''
						if worker.connection_time != 0:
							last_login = time.ctime(worker.connection_time)
						
						print >> f, "%16f, \t%50s, \t%13s, \t%24s, \t%24s" %(worker.active_time, worker.hostname, worker.status, last_login, dcnx_time)
			except Exception, e:
				with open("rpm_output.log","w") as f:
					print >> f, worker.hostname, e 
			last = now
			time.sleep(_logging_interval)

		cclient.close()


class ControllerDaemon(Daemon):

	def task_handler(self,tasks_new):
		for task_now in sorted(tasks_new):
			task_data, task_args = self.cclient.task_get(task_now)

			task_cmd = task_data[:4]
			if task_cmd == COMMANDS.SEND_EXPERIMENT:
				exp = Experiment.decode(task_args["experiment"])

				no_workers_total = 0
				for role in exp.roles:
					no_workers_total += role.no_workers

				worker_path_list = self.cclient.worker_allocate(no_workers_total)
				ready = []
				failed = False
				if worker_path_list:
					last = 0
					for role in exp.roles:
						remaining = role.no_workers
						i = 0
						while remaining:
							try:
								worker = self.cclient.worker_get(worker_path_list[last+i])
									
								print worker.hostname, "connecting"

								#Send experiment
								channel = Channel(hostname=worker.hostname, username=worker.username, password=worker.password, pkey=worker.pkey, timeout=_timeout) 
								
								remote_path = "worker/experiments"
								channel.chdir(remote_path)

								channel.run("mkdir -p %s" % exp.name)
								channel.chdir(exp.name)

								print worker.hostname, "sending experiment"
								
								channel.put(_local_experiments_dir+exp.filename, exp.filename)

								#all experiments files must be gzipped
								channel.run("tar -xzf %s" % exp.filename)

								actor_id = self.cclient.exp_create_actor(exp.id, worker.path, role.id)
								channel.run("echo \"parameters=%s\nexp_id=%s\nrole_id=%s\nactor_id=%s\nis_snapshot=%s\" > info.cfg" % (role.parameters, exp.id, role.id, actor_id, exp.is_snapshot))
								
								channel.close()
								remaining-=1
								i+=1
								ready.append((worker.path,actor_id))
							except Exception,e:
								print worker.hostname,e
								new_worker = self.cclient.worker_allocate()
								if new_worker == []:
									failed = True
									break
								else:
									del worker_path_list[last+i]
									worker_path_list += new_worker
						
						if failed:
							break

						last += role.no_workers
				else:
					print exp.name,"Not enough workers available!"


				if failed:
					print exp.name,"Not enough workers available!"
					self.cclient.task_del(task_now)

				else:
					for i in xrange(no_workers_total):
						self.cclient.exp_ready_on_worker(exp.id, ready[i][0], ready[i][1])

					self.cclient.exp_start(exp.id)

					print exp.name, "run all!"

					self.cclient.task_del(task_now)
			
			elif task_cmd == COMMANDS.RECOVER_ACTOR:
				worker = Worker.decode(task_args["worker"])

				exp_list = self.cclient.worker_get_experiments(worker)
				channel = None

				#TODO: check worker status

				try:
					print worker.hostname, "connecting"

					channel = Channel(worker.hostname, username=worker.username, pkey = worker.pkey, password=worker.password, timeout=_timeout)
					
					channel.chdir("worker")
					channel.run("python %s stop" % (_worker_daemon))
					channel.run("python %s start" % (_worker_daemon))

					print worker.hostname,"daemon recovered"

					channel.close()
				except:

					print worker.hostname, 'unable to connect'

					worker_path_list = self.cclient.worker_allocate(len(exp_list))

					if worker_path_list:
						for i in xrange(len(exp_list)):
							exp = exp_list[i]
							w = worker_path_list[i]
							for role in exp.roles:
								if role.id == exp.actor.role_id:
								
									print w.hostname, "connecting"

									#Send experiment
									channel = Channel(hostname=w.hostname, username=w.username, password=w.password, pkey=w.pkey, timeout=_timeout) 
									
									remote_path = "worker/experiments"
									channel.chdir(remote_path)

									channel.run("mkdir -p %s" % exp.name)
									channel.chdir(exp.name)

									print w.hostname, "sending experiment"
									
									channel.put(_local_experiments_dir+exp.filename, exp.filename)

									#all experiments files must be gzipped
									channel.run("tar -xzf %s" % exp.filename)

									actor_id = self.cclient.exp_create_actor(exp.id, w.path, role.id, actor_path=exp.actor.path)
									channel.run("echo \"parameters=%s\nexp_id=%s\nrole_id=%s\nactor_id=%s\nis_snapshot=%s\" > info.cfg" % (role.parameters, exp.id, role.id, actor_id, exp.is_snapshot))
									
									self.cclient.exp_ready_on_worker(exp.id, w.path, actor_id)
									
									channel.close()

						print exp.name, "run all!"

						self.cclient.worker_remove_experiments(worker.path)
						self.cclient.worker_add_disconnected(worker.hostname, 'LOST IDLE')

					else:
						#ACTOR FAILURE!!
						#TODO: declare failed actor on experiment
						print worker.hostname,"Not enough workers available for reallocation!"

				self.cclient.task_del(task_now)

			elif task_cmd == COMMANDS.NEW_EXPERIMENT:
				exp = Experiment.decode(task_args["experiment"])

				exp.name = exp.name.replace(' ','_')

				self.cclient.exp_add(exp)

				self.cclient.task_add(COMMANDS.SEND_EXPERIMENT, experiment=exp)

				self.cclient.task_del(task_now)

			elif task_cmd == COMMANDS.NEW_WORKER:
				worker = Worker.decode(task_args["worker"])

				if not self.cclient.worker_check(worker.hostname):
		
					try:
						channel = Channel(worker.hostname, username=worker.username, pkey = worker.pkey, password=worker.password, timeout=_timeout)
												
						print worker.hostname,"is online"

						remote_path = "worker"

						channel.run("echo \"server=%s:%s\nhostname=%s\" > %s/info.cfg" % (get_ip(), _controllerport, worker.hostname,remote_path))
						
						self.cclient.worker_add(worker)

						self.cclient.task_add(COMMANDS.INSTALL_WORKER, worker=worker)
						
						channel.close()

					except Exception, e:
						print worker.hostname, e 
						#Unable to connect
						self.cclient.worker_remove(worker)					
				else:
					print worker.hostname, "hostname already registered!"
					#TODO: remove on final
					self.cclient.task_add(COMMANDS.INSTALL_WORKER, worker=worker)

				self.cclient.task_del(task_now)

			elif task_cmd == COMMANDS.INSTALL_WORKER:
				worker = Worker.decode(task_args["worker"])
				
				#Install daemon

				try:
					print worker.hostname,"connecting"

					channel = Channel(worker.hostname, username=worker.username, pkey = worker.pkey, password=worker.password, timeout=_timeout)
					
					remote_path = "worker"

					channel.run("mkdir -p %s/experiments" % remote_path)
					channel.chdir(remote_path)

					#INSTALL PYTHON (+ MAKE + GCC) 

					print worker.hostname,"downloading dependencies"
					channel.run("sudo yum install -y --nogpgcheck openssl-devel libffi-devel")

					
					#Python version output goes to the stderr interface (y tho?)
					_,stderr = channel.run("python -V")
					vers = stderr.read().strip()
					if int(vers.split(' ')[-1]) <= _pyvers:
						print worker.hostname,"installing Python %s + pip [+ gcc + make] (actual version = %s)" % (_pyvers,vers)

						channel.run("sudo yum install -y --nogpgcheck make gcc")
						channel.run("wget https://www.python.org/ftp/python/%s/Python-%s.tgz" % (_pyvers,_pyvers))
						channel.run("tar -xzf Python-%s.tgz" % _pyvers)
						channel.chdir("Python-%s" % _pyvers)

						channel.run("./configure --with-ensurepip=yes")
						channel.run("make")
						channel.run("sudo make install")
						channel.run("sudo pip install --upgrade pip")

						channel.chdir("~/worker")

						channel.run("rm -rf Python-%s*" % _pyvers)
						

					print worker.hostname, "python is up-to-date"

					print worker.hostname,"sending daemon and API"

					channel.put(_worklibtarfile,_worklibtarfile)
					channel.run("tar -xzf %s" % (_worklibtarfile))

					self.cclient.worker_add_disconnected(worker.hostname, "INSTALLED", is_failure=False)
					self.cclient.task_add(COMMANDS.START_WORKER, worker=worker)
					

				except Exception, e:
					print worker.hostname, e
					self.cclient.worker_add_disconnected(worker.hostname, "NOT INSTALLED")

				self.cclient.task_del(task_now)
				
			elif task_cmd == COMMANDS.START_WORKER:
				worker = Worker.decode(task_args["worker"])

				try:
					channel = Channel(worker.hostname, username=worker.username, pkey = worker.pkey, password=worker.password, timeout=_timeout)
					
					channel.chdir("worker")
					channel.run("python %s stop" % (_worker_daemon))
					channel.run("python %s start" % (_worker_daemon), async=True)

					print worker.hostname,"daemon running"

					channel.close()
				except Exception, e:
					print worker.hostname, e
					self.cclient.worker_add_disconnected(worker.hostname, 'TIMED IDLE' if self.cclient.worker_get_experiments(worker) == [] else 'TIMED BUSY')

				self.cclient.task_del(task_now)

			elif task_cmd == COMMANDS.EXIT:
				self.cclient.task_del(task_now)
				self.exit = True

	def run(self):
		self.cclient = ControllerClient()

		self.exit = False

		self.cclient.watch_new_tasks(self.task_handler)

		rpm = RPM()
		rpm.daemon = True
		
		subprocess.Popen(['python', 'webservice/manage.py', 'runserver', '0:3181'])

		while not self.exit:
			if not rpm.is_alive():
				rpm = RPM()
				rpm.daemon = True
				rpm.start()

			time.sleep(_logging_interval)

		rpm.terminate()
		web.kill()
		self.cclient.config_stop()

	def stop(self):
		super(ControllerDaemon,self).stop()
		#stops webservice
		subprocess.call('pkill -f "python webservice/manage.py runserver 0:3181"', shell=True)

if __name__ == '__main__':
	daemon_cmd = sys.argv[1]
	daemon = ControllerDaemon("/tmp/daemon_controller.pid", stdout = "/dev/stdout", stderr= "/dev/stderr")

	if daemon_cmd == 'start':
		daemon.start()
		daemon_pid = daemon.getpid()

		if not daemon_pid:
			print "Unable run daemon"
		else:
			print "Daemon is running [PID=%d]" % daemon_pid

	elif daemon_cmd == 'stop':
		print "Stoping daemon"
		daemon.stop()

	elif daemon_cmd == 'restart':
		print "Restarting daemon"
		daemon.restart()

	elif daemon_cmd == 'status':
		daemon_pid = daemon.getpid()

		if not daemon_pid:
			print "Daemon isn't running"
		else:
			print "Daemon is running [PID=%d]" % daemon_pid
