from django.http import HttpResponse, JsonResponse, HttpResponseRedirect
from django.views.decorators.csrf import csrf_exempt
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import JSONParser
from rest_framework.decorators import api_view
from .serializers import MeasurementsSerializer
from .models import Measurements, Devices, Patients, Pairings
from flatdict import FlatDict
from django.contrib.messages.views import SuccessMessageMixin
from django.views.generic.edit import CreateView, DeleteView
from django.views.generic.list import ListView
from django.views.generic.detail import DetailView
from datetime import datetime as dt

from django.urls import reverse_lazy, reverse
from .forms import PairingsForm #, UnPairingsForm
from django.contrib.auth.mixins import LoginRequiredMixin
from home.views import QS_AnyMeasure

class DashMixin(object):
    ''' adds context from all models to dashboard template '''
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['Nmeasures'] = Measurements.objects.all().count()
        context['Ndevices'] = Devices.objects.all().count()
        context['Npatients'] = Patients.objects.all().count()
        context['Npairings'] = Pairings.objects.all().count()
#       context['last_measures'] = Measurements.objects.all().order_by('-measurements_timestamp')[:10].values()
        context['Lpairs'] = Pairings.objects.all()
#       context['myip'] = self.request.get_host()
#       print(context['last_measures'])
        return context

class DashBoard(DashMixin, ListView): #LoginRequiredMixin,
    context_object_name = 'dash_list'
    template_name = 'itasc/home.html'
    queryset = ''

class MeasurementsListView(DashMixin, ListView):
    model = Measurements
    #fields = ["measurements_timestamp" ,  "device_imei" , "patientid"]
    template_name = 'itasc/measurements_list.html'
    paginate_by = 30

    def get_queryset(self, *args, **kwargs):
        qs = super(MeasurementsListView, self).get_queryset(*args, **kwargs)
        qs = QS_AnyMeasure(qs.order_by('-measurements_timestamp')[:15].values()).asdict()
        return qs

class PairingsListView(DashMixin, ListView):
    model = Pairings
    template_name = 'itasc/pairings_list.html'

class PairingsDetailView(DashMixin, DetailView): # not used
    model = Pairings

class PairingsCreateView(DashMixin, SuccessMessageMixin, CreateView): #LoginRequiredMixin
    model = Pairings
    form_class = PairingsForm
    success_url = reverse_lazy('itasc:pairings-list')
    template_name = 'itasc/pairings_create.html'

    def get_success_message(self, cleaned_data):
        return f"{cleaned_data} has been created successfully"

class PairingsDeleteView(DeleteView): #LoginRequiredMixin
    model = Pairings
    #form_class = UnPairingsForm
    #fields = ['subject', 'device']
    success_url = reverse_lazy('itasc:pairings-list')
    #success_message = 'Added pairing '
    template_name = 'itasc/pairings_confirm_delete.html'

    def get_success_message(self, cleaned_data):
        return f"{cleaned_data} has been deleted successfully"

class PatientsListView(DashMixin, ListView):
    model = Patients
    template_name = 'itasc/patients_list.html'
    success_url = reverse_lazy('itasc:patients_list')

class DevicesListView(DashMixin, ListView):
    model = Devices
    template_name = 'itasc/devices_list.html'

#@api_view(['GET', 'POST'])
@csrf_exempt
def webhook(request):
# GET API to return ALL or some Measurements
# POST to database, assign by custom save  to subject and  (add devices)

    if request.method == 'GET':
        last10 = Measurements.objects.all().order_by('-metadata_receivedtime')[:15] # .latest()
        serializer = MeasurementsSerializer(last10, many=True)
        return JsonResponse(serializer.data, safe=False)

    elif request.method == 'POST':
        data = FlatDict(JSONParser().parse(request), delimiter='_')
        data['metadata_deviceGroups'] = "N/A"
        post = Measurements()
        for (k,v) in data.items():
            # convert o/1 to False/True
            if v in ('true', 'True', 1, True):
                v = True
            if v in ('false', 'False', 0, False):
                v = False
            setattr(post, k.lower(), v) # post.device_imei = data['device_IMEI']
        try:
            post.save()
            with open('itasc.json', 'a') as f:
                f.write(f'\n db hit @ {dt.now().isoformat()}\n')
                for k,v in post.__dict__.items():
                    f.write(f"{k} {v}\n")
        except Exception as e:
            with open('itasc_error.log', 'a') as f:
                f.write(f'\n {dt.now().isoformat()}\n')
                f.write(f'\n unknown error during save {e}\n ')
        finally:
            # add any new devices
            Devices.objects.update_or_create(imei = post.device_imei)
            #return Response({"status": status.HTTP_200_OK, "data": data}, status=status.HTTP_200_OK)
        return HttpResponse(status=200)
