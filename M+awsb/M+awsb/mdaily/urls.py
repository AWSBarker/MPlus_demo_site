from django.urls import path
from .views import HomePage, ImeiByCountry, ActiveByCountry, View1, MorgListView1, D_1ListView1, Search1
from django_filters.views import FilterView
from .filters import MDailyFilter

app_name = 'mdaily'
urlpatterns = [
    path('', HomePage.as_view(), name='HomePage'),
    path('view1/', View1.as_view(), name='view1'),
    path('view2/', View1.as_view(), name = 'view2'),
    path('MorgListView1/', MorgListView1.as_view(), name = 'MorgListView1'),
    path('D_1ListView1/', D_1ListView1.as_view(), name ='D_1ListView1'),
    path('Search1/', FilterView.as_view(filterset_class=MDailyFilter), name='Search1'),
    path('report/', ActiveByCountry.as_view()),
    path('report2/', ImeiByCountry.as_view()),
]
