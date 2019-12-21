from django.conf.urls import url
from django.urls import path
from . import view

urlpatterns = [
    # path('map', view.map),
    url(r'^$', view.map, name='map'),
    #path('dashboard', view.dashboard),
    #path('connection', view.connection),
    path('hello', view.hello)
]