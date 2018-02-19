from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^$', views.sourceFile, name='source'),
    url(r'^scanFileUpload', views.scanFile, name='scan'),
    url(r'^mappingFileUpload', views.mappingFile, name='mapping'),
    url(r'^success', views.success, name='success'),
    url(r'^visualisation', views.visualisation, name='india'),

]