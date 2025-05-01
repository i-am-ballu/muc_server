from django.urls import path
from . import views

urlpatterns = [
    path('countries/', views.get_countries, name='get_countries'),
    path('states/', views.get_states, name='get_states'),
    path('cities/', views.get_cities, name='get_cities'),
]
