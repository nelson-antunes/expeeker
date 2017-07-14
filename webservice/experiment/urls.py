from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'add/', views.exp_add, name='exp_add'),
    url(r'list/', views.exp_list, name='exp_list'),
    url(r'show/(?P<exp_id>e\d+)/$', views.exp_show, name='exp_show'),
]
