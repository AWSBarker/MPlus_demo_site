from django.urls import path
from .views import View1, Index, TrialDetailView, Search1
from django_filters.views import FilterView
from .filters import Ctgov1Filter

app_name = 'trials'
urlpatterns = [
    path('', Index.as_view(), name = 'Index'),
    path('view1/', View1.as_view(), name='view1'),
    path('search/', FilterView.as_view(filterset_class=Ctgov1Filter, template_name='trials/search_trials.html'), name='Search1'),
    path('<str:pk>/', TrialDetailView.as_view(), name='trial_detail'),
]
