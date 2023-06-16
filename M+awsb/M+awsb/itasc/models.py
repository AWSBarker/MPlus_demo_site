from django.db import models
from django.urls import reverse
import pandas as pd
from datetime import datetime as dt

class PairedManager(models.Manager):
    ''' return only paired devices - Patients.device not 0'''
    def get_queryset(self):
        return super().get_queryset().exclude(pairing__device__gt=0)

class Devices(models.Model):
    ''' Devices add manually. Can add auto add via webhook measure'''
    imei = models.PositiveBigIntegerField(primary_key=True)

    def __str__(self):
        return str(self.imei)

    class Meta:
        #managed = False - removed to let django manage
        db_table = 'Devices'
        verbose_name = 'Devices'
        verbose_name_plural = "Devices"
        ordering = ['-imei']

class Patients(models.Model):
    ''' patients are paired into subjects in Pairings.
    Extra fields could be added but IDs HIPPA GDPR....!
    '''
    patientid = models.CharField(primary_key=True, max_length=20)

    def __str__(self):
        return self.patientid

    def get_absolute_url(self):
        return reverse('patient-detail', kwargs={'pk': self.pk})

    class Meta:
        #managed = False - removed to let django manage
        db_table = 'Patients'
        verbose_name = 'Patients'
        verbose_name_plural = "Patients"
        ordering = ['-patientid']
        #constraints = [models.UniqueConstraint(fields=['patientid', 'device'], name = 'paired')]

class Pairings(models.Model):
    ''' unique pairs of current patient - devices '''
    id = models.AutoField(primary_key=True)
    subject = models.OneToOneField(Patients, on_delete=models.DO_NOTHING, unique=True) #, related_name='paired_device')
    device = models.OneToOneField(Devices, on_delete=models.DO_NOTHING, unique=True) #, related_name='paired_patient')

    def __str__(self):
        return str(self.subject) + ' - ' + str(self.device)

    def get_absolute_url(self):
        # not used. admin view turned off
        return reverse('pairings-detail', kwargs={'pk': self.id})

    class Meta:
        #managed = False - removed to let django manage
        db_table = 'Pairings'
        verbose_name = 'Pairings'
        verbose_name_plural = "Parings"
        ordering = ['-subject']
        #constraints = [models.UniqueConstraint(fields=['subject', 'device'], name = 'paired')]

class Measurements(models.Model):
    measurements_timestamp = models.DateTimeField(null=True)
    metadata_correlationid = models.CharField(db_column='metadata_correlationId', primary_key=True, max_length=36, null=False)  # Field name made lowercase.
    metadata_receivedtime = models.DateTimeField(db_column='metadata_receivedTime', blank=True, null=True)  # Field name made lowercase.
    metadata_devicegroups = models.TextField(db_column='metadata_deviceGroups', blank=True, null=True)  # Field name made lowercase.
    metadata_measurementtype = models.TextField(db_column='metadata_measurementType', blank=True, null=True)  # Field name made lowercase.
    device_id = models.TextField(blank=True, null=True)
    device_serialnumber = models.TextField(db_column='device_serialNumber', blank=True, null=True)  # Field name made lowercase.
    device_imei = models.PositiveBigIntegerField(db_column='device_IMEI', null=True)  # Field name made lowercase.
    device_imsi = models.TextField(db_column='device_IMSI', blank=True, null=True)  # Field name made lowercase.
    device_manufacturer = models.TextField(blank=True, null=True)
    device_model = models.TextField(blank=True, null=True)
    device_timezone = models.TextField(blank=True, null=True)

    measurements_annotations_averagemeasurement = models.BooleanField(db_column='measurements_annotations_averageMeasurement', blank=True, null=True)  # Field name made lowercase.
    measurements_annotations_irregularheartbeat = models.IntegerField(db_column='measurements_annotations_irregularHeartBeat', blank=True, null=True)  # Field name made lowercase.
    measurements_diastolicbloodpressure_value = models.IntegerField(db_column='measurements_diastolicBloodPressure_value', blank=True, null=True)  # Field name made lowercase.
    measurements_diastolicbloodpressure_unit = models.TextField(db_column='measurements_diastolicBloodPressure_unit', blank=True, null=True)  # Field name made lowercase.
    measurements_diastolicbloodpressure_isinrange = models.BooleanField(db_column='measurements_diastolicBloodPressure_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_meanbloodpressure_value = models.IntegerField(db_column='measurements_meanBloodPressure_value', blank=True, null=True)  # Field name made lowercase.
    measurements_meanbloodpressure_unit = models.TextField(db_column='measurements_meanBloodPressure_unit', blank=True, null=True)  # Field name made lowercase.
    measurements_meanbloodpressure_isinrange = models.BooleanField(db_column='measurements_meanBloodPressure_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_pulse_value = models.IntegerField(blank=True, null=True)
    measurements_pulse_unit = models.TextField(blank=True, null=True)
    measurements_pulse_isinrange = models.BooleanField(db_column='measurements_pulse_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_systolicbloodpressure_value = models.BigIntegerField(db_column='measurements_systolicBloodPressure_value', blank=True, null=True)  # Field name made lowercase.
    measurements_systolicbloodpressure_unit = models.TextField(db_column='measurements_systolicBloodPressure_unit', blank=True, null=True)  # Field name made lowercase.
    measurements_systolicbloodpressure_isinrange = models.BooleanField(db_column='measurements_systolicBloodPressure_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_ambienttemperature_value = models.FloatField(db_column='measurements_ambientTemperature_value', blank=True, null=True)  # Field name made lowercase.
    measurements_ambienttemperature_unit = models.TextField(db_column='measurements_ambientTemperature_unit', blank=True, null=True)  # Field name made lowercase.
    measurements_ambienttemperature_isinrange = models.BooleanField(db_column='measurements_ambientTemperature_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_annotations_codenumber = models.TextField(db_column='measurements_annotations_codeNumber', blank=True, null=True)  # Field name made lowercase.
    measurements_annotations_mealtag = models.TextField(db_column='measurements_annotations_mealTag', blank=True, null=True)  # Field name made lowercase.
    measurements_glucose_value = models.FloatField(blank=True, null=True)
    measurements_glucose_unit = models.TextField(blank=True, null=True)
    measurements_glucose_isinrange = models.BooleanField(db_column='measurements_glucose_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_bodycomposition_value = models.FloatField(db_column='measurements_bodyComposition_value', blank=True, null=True)  # Field name made lowercase.
    measurements_bodycomposition_unit = models.TextField(db_column='measurements_bodyComposition_unit', blank=True, null=True)  # Field name made lowercase.
    measurements_bodycomposition_isinrange = models.BooleanField(db_column='measurements_bodyComposition_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_bodyweight_value = models.FloatField(db_column='measurements_bodyWeight_value', blank=True, null=True)  # Field name made lowercase.
    measurements_bodyweight_unit = models.TextField(db_column='measurements_bodyWeight_unit', blank=True, null=True)  # Field name made lowercase.
    measurements_bodyweight_isinrange = models.BooleanField(db_column='measurements_bodyWeight_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_cholesterol_value = models.FloatField(blank=True, null=True)
    measurements_cholesterol_unit = models.TextField(blank=True, null=True)
    measurements_cholesterol_isinrange = models.BooleanField(db_column='measurements_cholesterol_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_uricacid_value = models.FloatField(blank=True, null=True)
    measurements_uricacid_unit = models.TextField(blank=True, null=True)
    measurements_uricacid_isinrange = models.BooleanField(db_column='measurements_uricacid_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_ketone_value = models.FloatField(blank=True, null=True)
    measurements_ketone_unit = models.TextField(blank=True, null=True)
    measurements_ketone_isinrange = models.BooleanField(db_column='measurements_ketone_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_hematocrit_value = models.FloatField(blank=True, null=True)
    measurements_hematocrit_unit = models.TextField(blank=True, null=True)
    measurements_hematocrit_isinrange = models.BooleanField(db_column='measurements_hematocrit_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_temperature_value = models.FloatField(blank=True, null=True)
    measurements_temperature_unit = models.TextField(blank=True, null=True)
    measurements_temperature_isinrange = models.BooleanField(db_column='measurements_temperature_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_spo2_value = models.IntegerField(db_column='measurements_SpO2_value', blank=True, null=True)  # Field name made lowercase.
    measurements_spo2_unit = models.TextField(db_column='measurements_SpO2_unit', blank=True, null=True)  # Field name made lowercase.
    measurements_spo2_isinrange = models.BooleanField(db_column='measurements_SpO2_isInRange', blank=True, null=True)  # Field name made lowercase.
    measurements_device_id = models.TextField(blank=True, null=True)
    measurements_device_serialnumber = models.IntegerField(db_column='measurements_device_serialNumber', blank=True, null=True)  # Field name made lowercase.
    measurements_device_imei = models.IntegerField(db_column='measurements_device_IMEI', blank=True, null=True)  # Field name made lowercase.
    measurements_device_manufacturer = models.TextField(blank=True, null=True)
    measurements_device_model = models.TextField(blank=True, null=True)
    measurements_device_timezone = models.TextField(blank=True, null=True)
    measurements_ecgsamples_minvalue = models.SmallIntegerField(db_column='measurements_ecgSamples_minValue', null=True)  # Field name made lowercase.
    measurements_ecgsamples_maxvalue = models.SmallIntegerField(db_column='measurements_ecgSamples_maxValue', null=True)  # Field name made lowercase.
    measurements_ecgsamples_samplerate = models.SmallIntegerField(db_column='measurements_ecgSamples_sampleRate', null=True)  # Field name made lowercase.
    measurements_ecgsamples_samplerateunit = models.TextField(db_column='measurements_ecgSamples_sampleRateUnit', null=True)  # Field name made lowercase.
    measurements_ecgsamples_factor = models.FloatField(db_column='measurements_ecgSamples_factor', null=True)  # Field name made lowercase.
    measurements_ecgsamples_factorunit = models.TextField(db_column='measurements_ecgSamples_factorUnit', null=True)  # Field name made lowercase.
    measurements_ecgsamples_samples = models.TextField(db_column='measurements_ecgSamples_samples', null=True)  # Field name made lowercase.
    device_additionalattributes_currentdevicetime = models.FloatField(db_column='device_additionalAttributes_currentDeviceTime', blank=True, null=True)  # Field name made lowercase.
    device_additionalattributes_devicetype = models.TextField(db_column='device_additionalAttributes_deviceType', blank=True, null=True)  # Field name made lowercase.
    device_additionalattributes_devicever = models.TextField(db_column='device_additionalAttributes_deviceVer', blank=True, null=True)  # Field name made lowercase.

    patientid = models.CharField(max_length=20, null=True)

    class Meta:
        #managed = False - removed to let django manage
        db_table = 'Measurements'
        verbose_name = 'Measurements'
        verbose_name_plural = "Measurements"
        ordering = ['-measurements_timestamp']

# MUST have at any time unique pairing patient-device. Case where device not assigned?
    def save(self, *args, **kwargs):
        try:
            self.measurements_timestamp = pd.to_datetime(self.measurements_timestamp, format='%Y-%m-%dT%H:%M:%S%Z').replace(tzinfo=None)
        except Exception as e:
            self.measurements_timestamp = pd.to_datetime(self.measurements_timestamp, format='%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=None)
        else :
            self.measurements_timestamp = pd.to_datetime(self.measurements_timestamp, format='%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=None)

        try :
            self.metadata_receivedtime = pd.to_datetime(self.metadata_receivedtime, format='%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=None)
        except:
            pass

        try:
            self.patientid = Pairings.objects.get(device = self.device_imei).subject.patientid
        except Pairings.DoesNotExist:
            with open('itasc.json', 'a') as f:
                f.write(f'\n {dt.now().isoformat()}\n')
                f.write(f'NOTE: patientid not found with {self.device_imei}\n ')
            self.patientid = None

        super().save(*args, **kwargs)  # Call the "real" save() method.

"""
    def save(self, *args, **kwargs):
        # 'measurements_timestamp': '2022-07-17T16:16:39Z'
        # 'metadata_receivedTime': '2022-07-17T16:16:39.791797Z'
        # PM100 PDF (img) and RAW (string) to measurements_ecgSamples_samples

        self.measurements_timestamp = self.measurements_timestamp[:-1].replace('T', ' ')
        self.metadata_receivedtime = self.metadata_receivedtime[:-1].replace('T', ' ')

        if self.device_model == 'BS-2001-G1': # remove col 'measurements_annotations_measuredUnit'
            self.measurements_annotations_measuredunit = None

        elif self.device_model == 'PM100': # convert PDF to img, RAW to string save to measurements_ecgSamples_samples)
            if self.measurements_documents_ecgreportbase64pdf:
                base64_img = self.measurements_documents_ecgreportbase64pdf
                self.measurements_ecgsamples_samples = base64_img #.encode('utf-8')
                #self.measurements_documents_ecgreportbase64pdf = None
            else: # RAW
                self.measurements_ecgsamples_samples = json.dumps(self.measurements_ecgsamples_samples) #json.dumps(data['measurements']["ecgSamples"]['samples']))

# GW error  Field 'measurements_device_serialnumber' expected a number but got '825522022026250B'.
        with open('json.json', 'a') as f:
            f.write(f'\n db hit @ {dt.now().isoformat()}\n')
            f.write(f'Recieved @: {self.metadata_receivedtime}  Measured @ {self.measurements_timestamp}\n')
            for k,v in self.__dict__.items():
                f.write(f"{k} {v}\n")

        super().save(*args, **kwargs)  # Call the "real" save() method.
"""

