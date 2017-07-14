from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'add/', views.worker_add, name='worker_add'),
    url(r'list/', views.worker_list, name='worker_list'),
    url(r'show/(?P<worker_hostname>[A-Za-z0-9.\-_]+)', views.worker_show, name='worker_show'),
]
