#
#   @author: Nelson Antonio Antunes Junior
#   @email: nelson.a.antunes at gmail.com
#   @date: (DD/MM/YYYY) 27/01/2017

import sys, time, logging, time, traceback
from extralib.daemon import Daemon

logging.basicConfig(filename='logging.out',format='%(asctime)-15s %(levelname)s %(message)s', 
level=logging.DEBUG) 

try:
	from worklib.worker_client import *
except ImportError, e:
	import site, pip
	pip.main(["install", '--user', "kazoo==2.4"])
	reload(site)
	from worklib.worker_client import *


class WorkerDaemon(Daemon):
	def run(self):
		sys.stdout = Printer()
		sys.stdout.add(open('log.out','w+',1))
		sys.stderr = Printer()
		sys.stderr.add(open('log.err','w+',1))
		cfg = WorkerClient.load_config_file("info.cfg")
		wclient = WorkerClient(cfg["server"], cfg["hostname"])
		wclient.exp_load()
		sleep_interval = 30
		last_timestamp = time.time()

		while True:
			busy = False
			actual_timestamp = time.time()
			try:
				for exp_obj in wclient.current_experiments:
					busy = True
					if exp_obj.is_finished():
						wclient.exp_finished(exp_obj)
					elif exp_obj.is_running() and exp_obj.is_snapshot:
						exp_obj.snapshot._save()
					elif not (exp_obj.is_finished() or exp_obj.is_running()) and exp_obj.is_started():
						exp_obj.run(WorkerClient(cfg["server"]))

				if wclient.current_experiments == []:
					busy = False

				wclient.worker_keep_alive(actual_timestamp -last_timestamp, busy)

			except:
				traceback.print_exc()

			last_timestamp = actual_timestamp
			time.sleep(sleep_interval)


if __name__ == '__main__':
	daemon_cmd = sys.argv[1]
	daemon = WorkerDaemon("/tmp/daemon_worker.pid")

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
