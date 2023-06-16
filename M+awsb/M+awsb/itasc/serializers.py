from rest_framework import serializers
from .models import Measurements

class MeasurementsSerializer(serializers.ModelSerializer):

    class Meta:
        model = Measurements
        fields = (
                  "device_model", "device_imei" ,  "device_timezone",
                  "measurements_timestamp" ,  "metadata_correlationid" , "metadata_receivedtime" ,
                  "measurements_annotations_irregularheartbeat" ,
                  "measurements_systolicbloodpressure_value",
                  "measurements_diastolicbloodpressure_value" ,
                  "measurements_pulse_value",
                  "measurements_glucose_value",
                  "measurements_bodycomposition_value",
                  "measurements_bodyweight_value", "measurements_bodyweight_unit",
                  "measurements_cholesterol_value", "measurements_cholesterol_unit",
                  "measurements_uricacid_value", "measurements_uricacid_unit",
                  "measurements_ketone_value", "measurements_ketone_unit",
                  "measurements_hematocrit_value", "measurements_hematocrit_unit",
                  "measurements_temperature_value", "measurements_temperature_unit",
                  "measurements_spo2_value", "measurements_spo2_unit" )

#        fields = '__all__' #('metadata_correlationid','measurements_timestamp','device_imei') #, 'device_imsi') #'__all__'
#        fields = ('metadata_correlationid','measurements_timestamp','device_imei') #, 'device_imsi') #'__all__'

"""
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

"""
