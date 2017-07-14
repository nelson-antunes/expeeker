#
#	@author: Nelson Antonio Antunes Junior
#	@email: nelson.a.antunes at gmail.com
#	@date: (DD/MM/YYYY) 24/01/2017

import os, sys, getopt, tarfile, getpass
from conlib.controller_client import ControllerClient
from conlib.remote_access import Channel

#HELP USAGE
def _help():
		print 'CONTROLLER INSTALLATION SCRIPT'
		print 'by Nelson A. Antunes Jr. (09/2016)\n'
		print 'USAGE: python install_controller.py [OPTIONS] <host_address>'
		print 'OPTIONS:'
		print '-h,	--help		print this usage instrunctions'
		print '-u,	--user=		username that is used to login the host'
		print '-f,	--force		force the project files to be updated'
		print '-i, 	--identity= identity/private key file used to login'


#GLOBALS FOR SSH CONNECTION
setup_dir = "controller/"
username = ""
password = ""
timeout = 20
force = False

#GLOBALS FOR ZOOKEEPER
_zkversion = "zookeeper-3.4.9"
_zktarfile =  "%s.tar.gz" % (_zkversion)
_zkdownloadlink = "http://www-us.apache.org/dist/zookeeper/%s/%s" % (_zkversion,_zktarfile)

#PROJECT COMPRESSING
controller_libname = "conlib"
controller_tarfile = "%s.tar.gz" % (controller_libname)
controller_files = ["conlib",
				"webservice",
				"model",
				"controller_init.py",
				"daemon_controller.py"]
				
controller_executable = "controller_init.py"

worker_libname = "worklib"
worker_tarfile = "%s.tar.gz" % (worker_libname)
worker_files = ["worklib",
				"daemon_worker.py"]

extra_files = ["extralib"]

def _log(src,msg):
	print src, msg

def update_tarfiles():
	with tarfile.open(controller_tarfile, "w:gz") as tar:
		for f in extra_files:
			tar.add(f)
		for f in controller_files:
			tar.add(f)
	with tarfile.open(worker_tarfile, "w:gz") as tar:
		for f in extra_files:
			tar.add(f)
		for f in worker_files:
			tar.add(f)

#SENDING NECESSARY FILES FOR THE PROGRAM (PROJECT AND ZOOKEEPER)
def send_files(channel, server_address):
	try:
		#SETTING DIRECTORY
		channel.mkdir(setup_dir)
		channel.chdir(setup_dir)
		
		#DOWNLOADING ZK
		if not channel.chkfile(_zktarfile):
			_log(server_address, "Downloading %s." % (_zktarfile))
			channel.run("wget %s" % (_zkdownloadlink))

			#UNCOMPRESSING ZK
			channel.run("tar -k -xzf %s" % (_zktarfile))
			_log(server_address, "Download complete!")

			#SETTING ZK CONFIGURATION FILE
			channel.put('zoo.cfg',_zkversion+'/conf/zoo.cfg')

		#SENDING PROJECT
		_log(server_address, "Sending %s" % (controller_tarfile))
		channel.put(controller_tarfile,controller_tarfile)
			
		_log(server_address, "Sending %s" % (worker_tarfile))
		channel.put(worker_tarfile,worker_tarfile)

		#UNCOMPRESSING PROJECT
		opt = "-k"
		global force
		if force:
			opt = ""
		stdout,stderr = channel.run("tar %s -xzf %s" % (opt,controller_tarfile))

	except Exception, e:
		_log(server_address,e)

	channel.chdir("~")

#RUNNING ZK AND PROJECT
def run_controller(channel, server_address):
	try:
		channel.chdir(setup_dir)

		#RUNNING PROJECT
		channel.run("python %s" % (controller_executable))
		_log(server_address,"Controller running")
		
	except Exception, e:
		_log(server_address,e)

	channel.chdir("~")

#PROGRAM STARTS HERE
def main(argv = None):
	if argv is None:
		argv = sys.argv
	pkey = ''
	global username
	#CHECKING FOR OPTIONS AND ARGUMENTS
	try:
		#getopts syntax: "hlu:" == 'h' and 'l' are normal options, and 'u' needs an argument
		opts, args = getopt.getopt(argv[1:], 'hu:i:f', ['help', 'user=', 'force','identity='])
	except getopt.error, e:
		_log(__file__, e)
		_log(__file__, 'For help use --help')
		return 1	#error return code
	for o, a in opts:
		if o in ('-h', '--help'):
			_help()
			return 0	#ok return code
		elif o in ('-u', '--user'):
			if a != "":
				username = a
			else:
				_log(__file__, "Expected an argument after %s option" % (o))
				return 1

		elif o in ('-i', '--identity'):
			if a != "":
				pkey = open(a,'r')
			else:
				_log(__file__, "Expected an argument after %s option" % (o))
				return 1

		elif o in ('-f','--force'):
			global force
			force = True

	if not args:
		_log(__file__, 'Remote address missing')
		_log(__file__, 'For help use --help')
		return 1

	#INITIALIZATION
	servers = args
	update_tarfiles()
	if username != '':
		password = getpass.getpass()

	for server_address in servers:
		#CONNECTING TO REMOTE MACHINE
		try:
			_log(server_address, "Connecting.")
			channel = Channel(server_address, username = username,
				password = password, pkey=pkey, timeout = timeout)
		except Exception, e:
			_log(server_address,e)
			return 1

		#RUNNING INSTALLATION
		send_files(channel,server_address)
		run_controller(channel,server_address)

		#CLOSING CONNECTION
		channel.close()

	#EXITING
	return 0

if __name__ == "__main__":
    sys.exit(main())