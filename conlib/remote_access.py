#
#	@author: Nelson Antonio Antunes Junior
#	@email: nelson.a.antunes at gmail.com
#	@date: (DD/MM/YYYY) 24/01/2017

import paramiko, paramiko.rsakey, cStringIO, scp, hashlib, os

def MD5(filename):
	hash_md5 = hashlib.md5()
	with open(filename, "rb") as f:
		for chunk in iter(lambda: f.read(4096), b""):
			hash_md5.update(chunk)
	return hash_md5.hexdigest()

class Channel(object):
	def __init__(self, hostname, username=None, password=None,
		pkey=None, timeout=None):

		self.ssh = paramiko.SSHClient()
		self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

		self.hostname = hostname
		self.username = username
		self.timeout = timeout
		if password == "":
			password = None
		if pkey is None or pkey == "":
			self.pkey = None
		else:
			self.pkey = paramiko.rsakey.RSAKey(file_obj=cStringIO.StringIO(pkey),password=password)
			password = None
		
		self.password = password	
		self.path = "~/"
		try:
			self.ssh.connect(hostname=self.hostname, username=self.username,
				password=self.password, pkey=self.pkey, timeout=self.timeout)
		except Exception, e:
			raise e
		self.scp = scp.SCPClient(self.ssh.get_transport())
		self.connected = True

	def _actual_path(self,path):
		return self.path+path

	def run(self, cmd, async=False):
		if not self.connected:
			return None,None,None
		cmd = "cd %s; %s" % (self.path, cmd)

		if async:
			return tuple(self.ssh.exec_command(cmd)[1:])

		_,stdout,stderr = self.ssh.exec_command(cmd)
		
		stdout.channel.recv_exit_status()
		stderr.channel.recv_exit_status()

		return (stdout,stderr)

	def chkdir(self, path):
		stdout,_ = self.run("[ -d %s ]" % path)
		return stdout.channel.recv_exit_status()==0

	def chkfile(self, path):
		stdout,_ = self.run("[ -f %s ]" % path)
		return stdout.channel.recv_exit_status()==0

	def chdir(self, path):
		if path[-1] != "/":
			path+= "/"
		if path[0] in ["/", "~"]:
			if self.chkdir(path):
				self.path = path
		else:
			if self.chkdir(path):
				self.path = self._actual_path(path)

	def mkdir(self, path):
		self.run("mkdir -p %s" % path)

	def _cmpfiles(self, local_path, remote_path):
		stdout,_ = self.run("md5sum %s" % remote_path)
		return stdout.read().split(" ")[0] != MD5(local_path)

	def put(self, local_path, remote_path):
		if self.connected and os.path.isfile(local_path):
			if self.chkfile(remote_path):
				if self._cmpfiles(local_path, remote_path):
					self.scp.put(local_path,self._actual_path(remote_path))
					return True
			else:
				self.scp.put(local_path,self._actual_path(remote_path))
				return True
		return False

	def get(self, remote_path, local_path):
		if self.connected and self.chkfile(remote_path):
			if os.path.isfile(local_path):
				if self._cmpfiles(local_path, remote_path):
					self.scp.get(self._actual_path(remote_path),local_path)
					return True
			else:
				self.scp.get(self._actual_path(remote_path),local_path)
				return True
		return False

	def close(self):
		if self.connected:
			return self.ssh.close()
		return None
