from django.shortcuts import render
from django.http import HttpResponseRedirect

import os, sys
sys.path.insert(0, os.getcwd())

from .forms import AddExpForm, AddRoleForm
from django.forms import formset_factory
from conlib.controller_client import ControllerClient, COMMANDS
from model.Experiment import Experiment
from model.Role import Role

def exp_list(request):
	cclient = ControllerClient()
	results = cclient.exp_get_all()

	return render(request, 'experiment/list.html', {'results': sorted(results, key= lambda x: x.name)})

def exp_add(request):

	AddRoleFormSet = formset_factory(AddRoleForm, min_num=1, extra=4)

	if request.method == 'POST':

		exp_form = AddExpForm(request.POST, request.FILES)
		role_forms = AddRoleFormSet(request.POST)
		
		if exp_form.is_valid() and role_forms.is_valid():
			name = exp_form.cleaned_data['name']
			fileobj = request.FILES['tarfile']
			is_snapshot = exp_form.cleaned_data['is_snapshot']
			roles = []
			for role_form in role_forms:
				role_dict = role_form.cleaned_data
				if role_dict != {}:
					roles.append(Role(role_dict["name"], role_dict["parameters"], role_dict["number_of_workers"]))

			exp = Experiment(name, fileobj.name, roles, is_snapshot)
			exp.save_file(fileobj)

			cclient = ControllerClient()
			cclient.task_add(COMMANDS.NEW_EXPERIMENT, experiment=exp)

			return HttpResponseRedirect('/thanks/')


	else:
		exp_form = AddExpForm()
		role_forms = AddRoleFormSet()

	return render(request, 'experiment/add.html', {'exp_form': exp_form, 'role_forms': role_forms})

def exp_show(request, exp_id):
	cclient = ControllerClient()
	experiment = cclient.exp_get_by_id(exp_id)
	return render(request, 'experiment/show.html', {'experiment': experiment})