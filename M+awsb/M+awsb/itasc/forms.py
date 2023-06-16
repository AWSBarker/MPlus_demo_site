from django.forms import ModelForm
from itasc.models import Pairings, Devices, Patients

class PairingsForm(ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['device'].queryset = Devices.objects.filter(pairings__subject=None)
        self.fields['subject'].queryset = Patients.objects.filter(pairings__device=None)
        self.fields['device'].label = 'Unpaired Devices'
        self.fields['subject'].label = 'Unpaired Patients'

    class Meta:
        model = Pairings
        fields =['subject', 'device']

"""
class UnPairingsForm(ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['device'].queryset = Devices.objects.exclude(pairings__subject=None)
        self.fields['subject'].queryset = Patients.objects.exclude(pairings__device=None)
        self.fields['device'].label = 'paired Devices'
        self.fields['subject'].label = 'paired Patients'

    class Meta:
        model = Pairings
        fields =['subject', 'device']
"""
