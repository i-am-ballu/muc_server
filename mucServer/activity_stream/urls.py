from django.urls import path
from . import views

urlpatterns = [
    path('getDistributionBasedOnUserId/', views.getDistributionBasedOnUserId),
    path('getSupportDetailsBasedOnCompany/', views.getSupportDetailsBasedOnCompany),
    path('getActivityStreamBasedOnCompany/', views.getActivityStreamBasedOnCompany)
]
