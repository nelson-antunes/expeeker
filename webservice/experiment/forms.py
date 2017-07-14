from django import forms


class AddRoleForm(forms.Form):
    name = forms.CharField(label='Role', max_length=100)
    parameters = forms.CharField(label='Parameters', max_length=1000)
    number_of_workers = forms.IntegerField(min_value=0)

class AddExpForm(forms.Form):
    name = forms.CharField(label='Name', max_length=100)
    tarfile = forms.FileField(label='Project gzipped file')
    is_snapshot = forms.BooleanField(label='Is Snapshot?', required=False)
