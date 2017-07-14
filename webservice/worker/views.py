from django.shortcuts import render
from django.http import HttpResponseRedirect

import os, sys, time
sys.path.insert(0, os.getcwd())

from .forms import AddHostForm
from conlib.controller_client import ControllerClient, COMMANDS
from model.Worker import Worker

def worker_list(request):
	cclient = ControllerClient()
	results = cclient.worker_get_all() 
	results.sort(key= lambda x: x.hostname)

	return render(request, 'worker/list.html', {'results': sorted(results,key= lambda x: x.status)})

def worker_add(request):
	# if this is a POST request we need to process the form data
	if request.method == 'POST':
		# create a form instance and populate it with data from the request:
		form = AddHostForm(request.POST)
		# check whether it's valid:
		if form.is_valid():
			hostnames = form.cleaned_data['hostname']
			username = form.cleaned_data['username']
			host_key = form.cleaned_data['host_key']
			key_passwd = form.cleaned_data['key_passwd']
			
			cclient = ControllerClient()

			for host in hostnames.replace("\r","").split("\n"):
				if host != "":
					cclient.task_add(COMMANDS.NEW_WORKER, worker=Worker(host, username, password=key_passwd, pkey=host_key))
			
			return HttpResponseRedirect('/thanks/')

	# if a GET (or any other method) we'll create a blank form
	else:
		form = AddHostForm()

	return render(request, 'worker/add.html', {'form': form})

def worker_show(request, worker_hostname):
	cclient = ControllerClient()
	try:
		worker = cclient.worker_get_by_hostname(worker_hostname)
	except:
		return HttpResponseRedirect('/notfound/')
	worker.connection_time = time.ctime(worker.connection_time) if worker.connection_time != 0 else 'N/A'
	worker.disconnection_time = time.ctime(worker.disconnection_time) if worker.disconnection_time != 0 else 'N/A'
	return render(request, 'worker/show.html', {'worker': worker})