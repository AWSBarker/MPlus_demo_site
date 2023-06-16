from .models import Ctgov1
import django_filters
from django.utils import timezone
from datetime import timedelta
from django.contrib.auth.mixins import LoginRequiredMixin


class Ctgov1Filter(LoginRequiredMixin, django_filters.FilterSet):
    nct_id = django_filters.CharFilter(lookup_expr='icontains')
    description = django_filters.CharFilter(lookup_expr='icontains')
    days = django_filters.DateRangeFilter(field_name='updated') #, method='get_past_n_days', label='Updated')

    class Meta:
        model = Ctgov1
        fields = ['alloc', 'description', 'days', 'nct_id']

    @property
    def qs(self):
        parent = super().qs
        #author = getattr(self.request, 'user', None)
        return parent.filter(drank_final__gte = 150)[:200]
