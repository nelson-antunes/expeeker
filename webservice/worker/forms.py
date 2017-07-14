
from django import forms

class AddHostForm(forms.Form):
    hostname = forms.CharField(label='Hostname', widget=forms.Textarea, max_length=10000)
    username = forms.CharField(label='Username', max_length=100)
    host_key = forms.CharField(label='Host key', widget=forms.Textarea, max_length=10000)
    key_passwd = forms.CharField(label='Key password', widget=forms.PasswordInput, max_length=100)