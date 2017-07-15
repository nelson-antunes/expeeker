#
#	@author: Nelson Antonio Antunes Junior
#	@email: nelson.a.antunes at gmail.com
#	@date: (DD/MM/YYYY) 24/01/2017

import sys, os, subprocess

try:
    import kazoo, paramiko, scp
except ImportError, e:
    import pip,site
    pip.main(["install", "--user", "django=1.10.5", "kazoo==2.4", "paramiko==2.2.1", "scp"])
    reload(site)
    
from conlib.controller_client import ControllerClient


def main():
    print "STARTING ZK"
    subprocess.call("./zookeeper-3.4.9/bin/zkServer.sh start", shell=True)

    print "CONNECTING ZK"
    cclient = ControllerClient()

    print "CREATRING BASIC ZNODES ZK"
    cclient.config_create_missing_paths()

    if not os.path.isdir("./experiments"):
        print "CREATING EXPERIMENTS FOLDER"
        os.mkdir("./experiments")

    subprocess.call("python daemon_controller.py restart", shell=True)

if __name__ == '__main__':
    sys.exit(main())
