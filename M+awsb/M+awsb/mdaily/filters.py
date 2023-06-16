from django import forms
from .models import MDaily, MOrg
import django_filters

class MDailyFilter(django_filters.FilterSet):
    #imei =django_filters.NumberFilter(lookup_expr='')
    last_measure_at = django_filters.DateTimeFromToRangeFilter(field_name='last_measure_at')
    org = django_filters.ModelMultipleChoiceFilter(queryset=MOrg.objects.all()
                        , widget=forms.CheckboxSelectMultiple)
    class Meta:
        model = MDaily
        fields = ['imei', 'org', 'last_measure_at' ]
