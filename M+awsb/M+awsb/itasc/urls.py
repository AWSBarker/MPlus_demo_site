from django.urls import path
from .views import webhook, PairingsCreateView, PairingsListView, PairingsDetailView, \
    PairingsDeleteView, MeasurementsListView, PatientsListView, DashBoard, DevicesListView

app_name = 'itasc'
urlpatterns = [
    path('', MeasurementsListView.as_view(), name='measurements-list'),
    path('bp/', webhook, name = 'webhook'),
    path('measures/', MeasurementsListView.as_view(), name='measurements-list'),
    path('patients/', PatientsListView.as_view(), name='patients-list'),
    path('pairings/', PairingsListView.as_view(), name='pairings-list'),
    path('pairings/create/', PairingsCreateView.as_view(), name='pairings-create'),
    path('pairings/<int:pk>/delete/', PairingsDeleteView.as_view(), name='pairings-delete'),
    path('pairings/<int:pk>/detail/', PairingsDetailView.as_view(), name='pairings-detail'),
    path('devices/', DevicesListView.as_view(), name='devices-list'),
#    path('login/',LoginView.as_view(),name="login_url"),
#    path('register/',views.registerView,name="register_url"),
#    path('logout/',LogoutView.as_view(next_page='dashboard'),name="logout"),
]

