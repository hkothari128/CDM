from django.shortcuts import render
from django.http import HttpResponse, HttpResponseRedirect
from .forms import UploadFileForm
from django.conf import settings

import csv
import os


# Create your views here.

def sourceFile(request):
     template = 'fileUpload/sourceFileUpload.html'
     if request.method == 'POST':
         form = UploadFileForm(request.POST, request.FILES)
         if form.is_valid():
             handle_uploaded_file(request.FILES['file'])
             return HttpResponseRedirect('/scanFileUpload')
     else:
         form = UploadFileForm()
     return render(request, template, {'form': form})


def scanFile(request):
    template = 'fileUpload/scanFileUpload.html'
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            handle_uploaded_file(request.FILES['file'])
            return HttpResponseRedirect('/mappingFileUpload/')
    else:
        form = UploadFileForm()
    return render(request, template, {'form': form})


def mappingFile(request):
    template = 'fileUpload/mappingFileUpload.html'
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            handle_uploaded_file(request.FILES['file'])
            return HttpResponseRedirect('/success/')
    else:
        form = UploadFileForm()
    return render(request, template, {'form': form})

def visualisation(request):

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = BASE_DIR + '/media/dummydata.csv'

#    mydict = {}

    with open(file_path, mode='r') as infile:
        reader = csv.reader(infile)
        num = []
        budget = []
        preg = []
        states = []
        for row in reader:
            num.append(row[0])
            budget.append(row[1])
            preg.append(row[2])
            states.append(row[3])
        num.pop(0)
        budget.pop(0)
        preg.pop(0)
        states.pop(0)
    template = 'fileUpload/india.html'
    return render(request, template,{'state':states,'number':num,'pregnant':preg,'budg':budget,'dir':BASE_DIR})




def success(request):
    template = 'fileUpload/success.html'

    return render(request, template)

def handle_uploaded_file(res):
    req = res
