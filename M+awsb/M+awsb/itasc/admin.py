from django.contrib import admin
from .models import Devices, Patients, Measurements, Pairings
import csv
from django.http import HttpResponse
from home.views import QS_AnyMeasure

@admin.action(description='Export selected to CSV')
def export_csv(modeladmin, request, queryset):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="M+hubexport.csv"'
    writer = csv.writer(response)
    writer.writerow(modeladmin.fields) #['measurements_timestamp', 'metadata_receivedtime', 'device_imei', 'measurements_systolicbloodpressure_value','measurements_diastolicbloodpressure_value'])
    cs = queryset.values_list(*modeladmin.fields)
    writer.writerows(cs)
    return response

class PairingsAdmin(admin.ModelAdmin):
    fields = ('subject','device',)
    list_display = ('subject','device',)
    view_on_site = False

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        if db_field.name == "device":
            kwargs["queryset"] = Devices.objects.filter(pairings__subject=None)
        if db_field.name == "subject":
            kwargs["queryset"] = Patients.objects.filter(pairings__device=None)
        return super(PairingsAdmin, self).formfield_for_foreignkey(db_field, request, **kwargs)

class PatientsAdmin(admin.ModelAdmin):
    fields = ('patientid',)
    list_display = ('patientid','get_paired')
    list_select_related = True
    view_on_site = False

    @admin.display(description='paired with')
    def get_paired(self, obj):
        return obj.pairings.device


class MeasurementsAdmin(admin.ModelAdmin):
    fields = [f.name for f in Measurements._meta.get_fields()]
    list_display = ("device_model", "measurements_timestamp" ,  "device_imei" , "patientid", "val", 'sinrange', 'dinrange','ihb',)
    readonly_fields = fields
    list_filter = (['device_imei', "patientid", 'measurements_timestamp', 'device_model'])
    search_fields = (['device_imei'])
    ordering = ('-measurements_timestamp',)
    search_help_text = 'Search by IMEI (any digits)'
    empty_value_display = 'None'
    view_on_site = False
    actions = [export_csv]

    @admin.display(description='Measured')
    def val(self, obj):
        return QS_AnyMeasure(obj).asobj() # f"{obj.measurements_systolicbloodpressure_value}/{obj.measurements_diastolicbloodpressure_value} {obj.measurements_pulse_value} ({obj.measurements_annotations_irregularheartbeat})"

    @admin.display(description='Sys', boolean=True)
    def sinrange(self, obj):
        return obj.measurements_systolicbloodpressure_isinrange

    @admin.display(description='Dia', boolean=True)
    def dinrange(self, obj):
        return obj.measurements_diastolicbloodpressure_isinrange

    @admin.display(description='IHB', boolean=True)
    def ihb(self, obj):
        return not obj.measurements_annotations_irregularheartbeat

class DevicesAdmin(admin.ModelAdmin):
    fields = ('imei',)
    list_display = ('imei', 'get_paired')
    search_fields = (['imei'])
    view_on_site = False

    @admin.display(description='paired with')
    def get_paired(self, obj):
        return obj.pairings.subject

admin.site.register(Devices, DevicesAdmin)
admin.site.register(Pairings, PairingsAdmin)
admin.site.register(Patients, PatientsAdmin)
admin.site.register(Measurements, MeasurementsAdmin)
#admin.site.site_url = "/itasc"
# admin.site.site_header = "M+Hub webhook Admin for Users / Devices / Patients / Measurements"
# admin.site.site_title = "site_title>"
# admin.site.index_title = "Selection"
admin.site.disable_action('delete_selected')
