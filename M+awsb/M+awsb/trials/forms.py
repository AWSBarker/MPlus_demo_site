from django import forms

class TrialForm1(forms.Form):
    data = forms.CharField(label='data NCT03208010', required=True, max_length=12, )

class CustomForm1(forms.Form):
    data = forms.IntegerField(label='data',required=True)
